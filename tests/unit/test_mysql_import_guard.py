"""Proves `adapters/real/mysql_unit_of_work.py` imports cleanly with `pymysql`
absent, and that only *constructing* `MySqlUnitOfWork` in that state fails, with
this package's own `ConfigurationError` rather than a raw `ImportError` leaking
out of a driver.

Same mechanism and same reasoning as `test_sqs_event_bus_import_guard.py`:
`pymysql` is an optional extra, and a SQLite deployment must be able to import
the container -- which imports this module's siblings -- without installing a
MySQL driver it will never use.
"""

from __future__ import annotations

import importlib
import sys

import pytest
from shepherd_contracts.errors import ConfigurationError

import shepherd_workflow_service.adapters.real.mysql_unit_of_work as mysql_unit_of_work


@pytest.fixture
def pymysql_blocked():
    # Restored explicitly in the `finally`, in this order, rather than via
    # monkeypatch: monkeypatch's sys.modules undo runs AFTER this teardown, so
    # the reload below would run while pymysql was still blocked.
    saved = {
        name: sys.modules[name]
        for name in list(sys.modules)
        if name == "pymysql" or name.startswith("pymysql.")
    }
    for name in saved:
        del sys.modules[name]
    sys.modules["pymysql"] = None
    reloaded = importlib.reload(mysql_unit_of_work)
    try:
        yield reloaded
    finally:
        del sys.modules["pymysql"]
        sys.modules.update(saved)
        importlib.reload(mysql_unit_of_work)


def _kwargs(**overrides):
    base = {
        "host": "db.example",
        "port": 3306,
        "database": "shepherd_workflow",
        "user": "svc",
        "password_provider": lambda: "unused",
        "owned_tables": frozenset({"outbox"}),
    }
    base.update(overrides)
    return base


def test_module_imports_without_pymysql(pymysql_blocked) -> None:
    assert pymysql_blocked._pymysql is None


def test_constructing_without_pymysql_raises_configuration_error(pymysql_blocked) -> None:
    with pytest.raises(ConfigurationError, match="mysql"):
        pymysql_blocked.MySqlUnitOfWork(**_kwargs())


def test_an_injected_connect_does_not_need_the_driver(pymysql_blocked) -> None:
    """The driver is required only because it supplies the default `connect`.

    A caller that supplies its own -- a pooled connector, a proxy, a test double --
    must not be refused for lacking a dependency it does not use.
    """
    uow = pymysql_blocked.MySqlUnitOfWork(**_kwargs(connect=lambda **_: object()))

    assert uow.owned_tables() == frozenset({"outbox"})


def test_schema_ddl_is_exported_rather_than_executed_by_the_adapter() -> None:
    """Migrations own schema in this estate, so the adapter must not create tables.

    An adapter that quietly created its tables would race the migration that owns
    them and mask a missing deploy. The DDL is exported as `SCHEMA_STATEMENTS` for
    whoever does own migrations, and `session()` issues none of it.
    """

    class RecordingConnection:
        def __init__(self) -> None:
            self.cursor_calls = 0

        def cursor(self):
            self.cursor_calls += 1
            return None

    connection = RecordingConnection()
    uow = mysql_unit_of_work.MySqlUnitOfWork(**_kwargs(connect=lambda **_: connection))

    uow.session()

    assert connection.cursor_calls == 0
    # Positive control: the recorder can count, so the zero above is a measured
    # absence rather than a counter nobody ever incremented.
    connection.cursor()
    assert connection.cursor_calls == 1
    assert any(
        "CREATE TABLE IF NOT EXISTS outbox" in statement
        for statement in mysql_unit_of_work.SCHEMA_STATEMENTS
    )


def test_connection_opens_lazily_not_at_construction() -> None:
    opened: list[int] = []
    uow = mysql_unit_of_work.MySqlUnitOfWork(
        **_kwargs(connect=lambda **_: opened.append(1) or object())
    )

    assert opened == []  # constructing the unit of work does no I/O

    uow.session()

    assert opened == [1]


def test_repr_cannot_widen_into_a_credential() -> None:
    uow = mysql_unit_of_work.MySqlUnitOfWork(
        **_kwargs(password_provider=lambda: "hunter2", connect=lambda **_: object())
    )

    assert "hunter2" not in repr(uow)
    assert "password" not in repr(uow)


def test_module_uses_real_pymysql_once_available() -> None:
    """Sanity check that the fixture's teardown restored the module, so this test
    and everything after it see the real driver again."""
    assert mysql_unit_of_work._pymysql is not None
