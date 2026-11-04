"""A real `UnitOfWork` backed by MySQL 8, the engine this estate actually runs.

WHY BOTH THIS AND `SqliteUnitOfWork`. The SQLite adapter is the template's
zero-dependency local store: it needs no server, so `docker compose up` and a
bare `pytest` both work on a fresh checkout. Nothing in this estate deploys
SQLite. Every extracted service that persists anything lands on MySQL 8.0.46
(a local `mysql` Compose service, and a managed RDS instance in production), so
shipping only the SQLite pair meant the first real service would have to invent
its production adapter -- and invent it without a contract test, which is where
a port quietly stops being a port.

TWO DELIBERATE ASYMMETRIES WITH THE SQLITE ADAPTER, both load-bearing.

*This adapter issues no DDL.* `SqliteUnitOfWork.session()` runs
`CREATE TABLE IF NOT EXISTS` because its database is a scratch file that exists
only for the process that opened it. A MySQL instance is shared and versioned,
and this estate's rule is that migrations own schema (they live with the
application backend, never in the database IaC repository). An adapter
that silently creates tables would race the migration that owns them and mask a
missing deploy. `SCHEMA_STATEMENTS` below is the DDL, exported so a service's
migration -- and the contract test -- can apply it explicitly.

*The credential arrives through a callable, never through config.* A password on
a `ServiceConfig` field is one `repr()` away from a log line, and one
`subprocess` away from a process listing (see
`knowledge/wiki/secrets-on-argv-vs-liveness-probes.md`). `password_provider` is
resolved lazily, at first `session()`, so `build_container()` stays free of both
I/O and secret material -- `tests/unit/test_container.py` asserts the graph
builds with zero network access, and a constructor that called Secrets Manager
would break it.

`pymysql` is an optional dependency (`pyproject.toml`'s `mysql` extra) and is
imported at module scope behind a guard, exactly like `boto3` in
`adapters/real/sqs_event_bus.py`: importing this module must succeed without the
driver, and only CONSTRUCTING the unit of work fails, with this package's own
`ConfigurationError` rather than a vendor `ImportError`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from shepherd_contracts.errors import ConfigurationError

if TYPE_CHECKING:
    import pymysql  # noqa: F401 -- type-checking only, never imported at runtime here

try:
    import pymysql as _pymysql
except ImportError:  # pragma: no cover -- exercised via the import-guard test
    _pymysql = None


#: The DDL for the tables this template owns, in MySQL 8 flavour.
#:
#: `created_at` is `DATETIME(6)` holding UTC rather than `TIMESTAMP`: MySQL
#: converts a `TIMESTAMP` through the session time zone on the way in AND out, so
#: two services reading the same row with different `time_zone` settings disagree
#: about when an event happened. `DATETIME` stores what it was given. The
#: repository attaches UTC on read, which is where the timezone knowledge belongs.
#:
#: `utf8mb4` because `utf8` in MySQL is three bytes and cannot store an emoji or
#: much of the Arabic supplementary range -- a name column that silently truncates
#: is a data-loss bug that only shows up in production content.
SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS outbox (
        outbox_id    VARCHAR(191) NOT NULL,
        event_type   VARCHAR(191) NOT NULL,
        org_id       VARCHAR(191) NOT NULL,
        occurred_at  DATETIME(6)  NOT NULL,
        payload_json JSON         NOT NULL,
        relayed_at   DATETIME(6)  NULL,
        PRIMARY KEY (outbox_id),
        KEY idx_outbox_unrelayed (relayed_at, occurred_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
)


class MySqlUnitOfWork:
    """Lazily-connected MySQL transaction, owned by whoever constructed it."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        database: str,
        user: str,
        password_provider: Callable[[], str],
        owned_tables: frozenset[str],
        connect: Callable[..., Any] | None = None,
    ) -> None:
        if connect is None and _pymysql is None:
            raise ConfigurationError(
                "MySqlUnitOfWork requires the 'mysql' extra: "
                "pip install 'mshaker-shared-workflow[mysql]'"
            )
        if not host:
            raise ConfigurationError("MySqlUnitOfWork requires host")
        if not database:
            raise ConfigurationError("MySqlUnitOfWork requires database")
        if port <= 0:
            raise ConfigurationError(f"MySqlUnitOfWork requires a positive port, got {port!r}")
        self._host = host
        self._port = port
        self._database = database
        self._user = user
        self._password_provider = password_provider
        self._owned_tables = owned_tables
        self._connect = connect
        self._connection: Any | None = None

    def session(self) -> Any:
        if self._connection is None:
            connect = self._connect or _pymysql.connect  # type: ignore[union-attr]
            self._connection = connect(
                host=self._host,
                port=self._port,
                database=self._database,
                user=self._user,
                password=self._password_provider(),
                charset="utf8mb4",
                # Explicit, not inherited: `autocommit=True` would make `rollback()`
                # a no-op and quietly delete the transaction boundary this port
                # exists to express.
                autocommit=False,
            )
        return self._connection

    def owned_tables(self) -> frozenset[str]:
        return self._owned_tables

    def commit(self) -> None:
        if self._connection is not None:
            self._connection.commit()

    def rollback(self) -> None:
        if self._connection is not None:
            self._connection.rollback()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __repr__(self) -> str:
        # Explicit, because the default dataclass-ish repr of a future refactor
        # would print `password_provider` and a bound method's repr can name the
        # object it came from. Nothing here can widen into a credential.
        return (
            f"MySqlUnitOfWork(host={self._host!r}, port={self._port!r}, "
            f"database={self._database!r}, user={self._user!r})"
        )
