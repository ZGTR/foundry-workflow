"""`build_container()` picks the ledger's database from `config.db_backend`.

Three properties, and the third is the one that costs money to learn later:

* the default is unchanged (SQLite, no server, no driver);
* `mysql` builds the engine WITHOUT connecting, so the container graph stays
  free of I/O and of secret material; and
* an unrecognised backend is REFUSED, at BUILD time. There is no `else: sqlite`
  fall-through, because a service that asked for a database it did not get and
  was quietly handed a local file would look green while writing a durable
  ledger to storage nobody backs up
  (`knowledge/wiki/a-fall-through-inherits-the-branchs-severity.md`).

WHY THE REFUSAL HAS TO BE EAGER. The URL itself is resolved lazily, on the first
request, because reading a password through the secrets port is I/O and the
container must build without any. But whether the backend NAME is known is
answerable from the arguments alone, so it is answered at startup: a typo in
`DB_BACKEND` kills the process with the fix in the message rather than becoming
a 500 the first time someone submits a job. The last test below is the control
that both halves consult one authority instead of two lists that agree today.
"""

from __future__ import annotations

import socket

import pytest
from shepherd_contracts.errors import ConfigurationError

from shepherd_workflow_service.adapters.fake.secrets import FakeSecrets
from shepherd_workflow_service.adapters.real.sql_workflow_engine import SqlWorkflowEngine
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import _database_url, build_container
from tests.conftest import WORKFLOW_DEFINITION_PATH

_SECRET_ID = "shepherd/workflow/db-password"
_MYSQL_FIELDS = dict(
    db_backend="mysql",
    db_host="db.example",
    db_port=3306,
    db_name="shepherd_workflow",
    db_user="svc",
    db_password_secret_id=_SECRET_ID,
)


def _config(**overrides: object) -> ServiceConfig:
    fields: dict[str, object] = dict(
        env="test",
        port=8080,
        runtime_mode="http",
        issuer="test-issuer",
        db_path=":memory:",
        event_bus_mode="in_process",
        sqs_queue_url=None,
        aws_region=None,
        workflow_definition_path=WORKFLOW_DEFINITION_PATH,
    )
    fields.update(overrides)
    return ServiceConfig(**fields)  # type: ignore[arg-type]


def test_default_backend_is_sqlite() -> None:
    config = _config()

    container = build_container(config)

    assert isinstance(container.workflow_engine, SqlWorkflowEngine)
    assert _database_url(config, None) == "sqlite+pysqlite:///:memory:"


def test_mysql_backend_builds_the_engine_over_a_mysql_url() -> None:
    config = _config(**_MYSQL_FIELDS)
    secrets = FakeSecrets({_SECRET_ID: "s3cret"})

    container = build_container(config, secrets=secrets)

    assert isinstance(container.workflow_engine, SqlWorkflowEngine)
    assert _database_url(config, secrets).startswith("mysql+pymysql://svc:s3cret@db.example:3306/")


def test_mysql_backend_opens_no_socket_and_reads_no_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _no_sockets(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("build_container() must not open a network socket")

    monkeypatch.setattr(socket, "socket", _no_sockets)

    class ExplodingSecrets:
        def get(self, name: str) -> str:
            raise AssertionError(f"the credential {name!r} was resolved at build time")

    container = build_container(_config(**_MYSQL_FIELDS), secrets=ExplodingSecrets())

    assert container.workflow_engine is not None


def test_mysql_backend_without_secrets_is_refused() -> None:
    with pytest.raises(ConfigurationError, match="SecretsPort"):
        build_container(_config(**_MYSQL_FIELDS))


def test_unknown_backend_is_refused_rather_than_falling_back() -> None:
    with pytest.raises(ConfigurationError, match="unknown db_backend"):
        build_container(_config(db_backend="postgres"))


def test_mysql_config_missing_a_field_fails_at_construction() -> None:
    incomplete = dict(_MYSQL_FIELDS)
    del incomplete["db_name"]

    with pytest.raises(ValueError, match="db_name"):
        _config(**incomplete)


def test_the_lazy_url_builder_refuses_exactly_what_the_build_refused() -> None:
    """The control for the eager/lazy split above.

    Two separate branch lists -- one at build time, one at URL time -- would
    agree on the day they were written and diverge the day one of them learns a
    new backend, and the divergence would be invisible: the build would accept a
    backend the URL builder then rejected on the first request. Both call
    `_require_a_buildable_database`, so this asserts the second half refuses the
    same two cases rather than trusting that it was wired.
    """
    with pytest.raises(ConfigurationError, match="unknown db_backend"):
        _database_url(_config(db_backend="postgres"), None)

    with pytest.raises(ConfigurationError, match="SecretsPort"):
        _database_url(_config(**_MYSQL_FIELDS), None)
