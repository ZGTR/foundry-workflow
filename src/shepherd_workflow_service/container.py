"""The DI root. Explicit constructor wiring, no framework magic, no service
locator -- `build_container()` is a plain function that returns a plain
dataclass. A test overrides any single port by passing it as a keyword
argument; everything else builds from `config` alone.

Building the graph must never touch the network or the filesystem: every real
adapter here connects lazily (see `adapters.real.sqlite_unit_of_work`), so
`tests/unit/test_container.py::test_container_builds_with_zero_network_access`
can assert this by patching `socket.socket` to raise and calling
`build_container()` underneath it.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from urllib.parse import quote_plus

from shepherd_contracts.clock import Clock, SystemClock
from shepherd_contracts.errors import ConfigurationError
from shepherd_contracts.principal import CallerPrincipalV1, PrincipalMinter
from shepherd_workflow.dispatcher import Broker, DispatchReport

from shepherd_workflow_service.adapters.fake.header_principal_auth import HeaderPrincipalAuth
from shepherd_workflow_service.adapters.real.in_process_event_bus import InProcessEventBus
from shepherd_workflow_service.adapters.real.readiness import EngineReadinessCheck
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.ports.auth import PrincipalAuth
from shepherd_workflow_service.ports.event_bus import Publisher
from shepherd_workflow_service.ports.ledger_outbox import LedgerOutboxDrain
from shepherd_workflow_service.ports.readiness import ReadinessCheck
from shepherd_workflow_service.ports.secrets import SecretsPort
from shepherd_workflow_service.ports.workflow_engine import WorkflowEnginePort
from shepherd_workflow_service.services.workflow_service import WorkflowService

#: The database backends this service knows how to build a URL for. ONE list,
#: consulted both by the build-time refusal and by the lazy URL builder, so a
#: backend cannot be rejected in one place and accepted in the other.
_SUPPORTED_DB_BACKENDS = ("sqlite", "mysql")

#: Environments where the unverified header adapter is an acceptable default.
#: Enumerated deliberately: an unrecognised env is treated as production, so a
#: NEW environment name is secure by default rather than by remembering to add it.
LOCAL_ENVS = frozenset({"local", "test", "dev"})


class InsecureDefaultAuth(RuntimeError):
    """Raised when a non-local service would fall back to unverified header auth."""


@dataclass
class Container:
    config: ServiceConfig
    clock: Clock
    publisher: Publisher
    readiness_check: ReadinessCheck
    principal_auth: PrincipalAuth
    workflow_engine: WorkflowEnginePort
    workflow_service: WorkflowService
    #: The ledger outbox, and a LAZY factory for the broker its rows go to. The
    #: factory is not called here: building a boto3 client resolves credentials
    #: and a region, and `tests/unit/test_container.py` asserts the whole graph
    #: builds under a `socket.socket` that raises.
    ledger_outbox: LedgerOutboxDrain
    ledger_broker_factory: Callable[[], Broker]

    def authenticate(self, headers: Mapping[str, str]) -> CallerPrincipalV1:
        return self.principal_auth.authenticate(headers)

    def drain_ledger_outbox(self, *, batch_size: int = 100) -> DispatchReport:
        """Relay the workflow ledger's outbox once. Scheduled, never on a route.

        `src/shepherd_workflow_service/ledger_drain_main.py` is the process that
        calls this. The refusal below exists because the alternative is silent:
        an object with no `drain_pending` would otherwise have to be caught by an
        `AttributeError` somewhere further down, and "drained nothing" is
        byte-identical to "there was nothing to drain".
        """
        if not isinstance(self.ledger_outbox, LedgerOutboxDrain):
            raise ConfigurationError(
                f"the wired ledger outbox {type(self.ledger_outbox).__name__!r} has no "
                "drain_pending(), so this process cannot relay anything. Pass "
                "ledger_outbox=<adapter> to build_container(). Refusing rather than "
                "reporting an empty drain, which is indistinguishable from a real one."
            )
        return self.ledger_outbox.drain_pending(
            broker=self.ledger_broker_factory(), batch_size=batch_size
        )


def build_container(
    config: ServiceConfig,
    *,
    clock: Clock | None = None,
    publisher: Publisher | None = None,
    readiness_check: ReadinessCheck | None = None,
    principal_auth: PrincipalAuth | None = None,
    secrets: SecretsPort | None = None,
    workflow_engine: WorkflowEnginePort | None = None,
    ledger_outbox: LedgerOutboxDrain | None = None,
) -> Container:
    """Wire the whole graph from `config`. Every port can be overridden, e.g.
    `build_container(config, clock=FrozenClock(...))` in a test.
    """
    resolved_clock = clock or SystemClock()

    resolved_publisher = publisher or InProcessEventBus()

    # The engine is built BEFORE readiness because readiness now probes it. That
    # ordering is the whole reason `/readyz` means anything here: the ledger is
    # the only thing this service cannot serve a request without, so the readiness
    # probe has to ask the ledger and not some second store that happens to be up.
    # SECRETS ARE RESOLVED, NOT MERELY ACCEPTED. Both entrypoints -- `main.py` and
    # `ledger_drain_main.py` -- call `build_container(config)` with no `secrets=`,
    # because a process entrypoint has no adapter to hand it and inventing a second
    # database resolution in each one is the two-authorities defect this service's
    # config module exists to rule out. So every deployment on `DB_BACKEND=mysql`,
    # which is every deployment in this estate, died in `_require_a_buildable_database`
    # at startup, while `adapters/real/aws_secrets_manager.py` sat wired to nothing.
    #
    # This is the MIRROR of the resolution a sibling policy service reached for the same
    # port. There the SecretsPort had no consumer -- that service reads no database --
    # so the port, the adapter and the `aws_region` field were deleted as configured
    # behaviour that does not exist. Here the consumer is live: `_database_url` calls
    # `secrets.get(config.db_password_secret_id)` on the MySQL path. Same principle,
    # opposite answer: an adapter no composition root constructs is dead either way,
    # and the fix is to delete it or to construct it, never to leave it declared.
    resolved_secrets = secrets or _default_secrets(config)

    resolved_engine = workflow_engine or _build_workflow_engine(
        config, resolved_clock, resolved_secrets
    )

    resolved_readiness = readiness_check or EngineReadinessCheck(resolved_engine)

    # FAIL CLOSED ON AUTH. `HeaderPrincipalAuth` verifies no signature: it trusts
    # x-org-id and x-scopes as sent, so under it any caller is any tenant with any
    # scope. It is the right default for a local run and catastrophic anywhere
    # else, and the failure is silent -- a service that forgot to supply a real
    # adapter serves traffic and authorises every request.
    #
    # So the fallback is refused unless the environment is explicitly local. A
    # service that forgets real auth now dies in `build_container()`, at startup,
    # with the fix in the message. That is the whole point of the check: the
    # alternative is not "insecure", it is "insecure and green".
    if principal_auth is not None:
        resolved_auth: PrincipalAuth = principal_auth
    elif config.auth_jwks_uri and config.auth_audience:
        # Configured for real auth: verify signatures. Constructed eagerly but
        # the JWKS is fetched lazily, so this stays network-free at build time.
        from shepherd_workflow_service.adapters.real.jwt_principal_auth import (
            JwksKeyResolver,
            JwtPrincipalAuth,
        )

        resolved_auth = JwtPrincipalAuth(
            PrincipalMinter(issuer=config.issuer),
            JwksKeyResolver(config.auth_jwks_uri),
            issuer=config.issuer,
            audience=config.auth_audience,
        )
    elif config.env in LOCAL_ENVS:
        resolved_auth = HeaderPrincipalAuth(PrincipalMinter(issuer=config.issuer))
    else:
        raise InsecureDefaultAuth(
            f"env={config.env!r} has neither principal_auth wired nor AUTH_JWKS_URI/AUTH_AUDIENCE "
            "set, and the only remaining adapter (HeaderPrincipalAuth) trusts unverified request "
            "headers, so every caller would be able to assert any org and any scope. Set "
            "AUTH_JWKS_URI and AUTH_AUDIENCE, or pass principal_auth=<your adapter> to "
            f"build_container(), or set SERVICE_ENV to one of {sorted(LOCAL_ENVS)} if this is "
            "genuinely a local run."
        )

    return Container(
        config=config,
        clock=resolved_clock,
        publisher=resolved_publisher,
        readiness_check=resolved_readiness,
        principal_auth=resolved_auth,
        workflow_engine=resolved_engine,
        workflow_service=WorkflowService(engine=resolved_engine),
        # The SQL engine owns the ledger connection, so it is also the thing that
        # can drain the ledger's outbox. An injected engine that cannot is not
        # substituted for silently -- `drain_ledger_outbox()` refuses it by name.
        ledger_outbox=ledger_outbox if ledger_outbox is not None else resolved_engine,
        # `publisher`, NOT `resolved_publisher`. See `_ledger_broker_factory`.
        ledger_broker_factory=_ledger_broker_factory(config, publisher, resolved_clock),
    )


def _ledger_broker_factory(
    config: ServiceConfig, publisher: Publisher | None, clock: Clock
) -> Callable[[], Broker]:
    """Build the ledger drain's downstream lazily, and REFUSE a non-durable one.

    THE REFUSAL IS THE POINT, and it is why this takes the INJECTED `publisher`
    rather than the container's resolved one. That field falls back to
    `InProcessEventBus`, which delivers to an in-memory subscriber list -- empty
    in every deployed process, since nothing in this service subscribes. A drain
    wired to it would publish nothing, mark every row sent, and report a clean
    run. The outbox would empty and the events would be gone, which is strictly
    worse than today's undrained table, where at least the rows still exist.

    So: an explicitly injected publisher is honoured (a test's recorder), a
    configured SQS queue is used, and anything else raises rather than delivering
    to nowhere.
    """

    def factory() -> Broker:
        from shepherd_workflow_service.adapters.real.ledger_broker import PublisherBroker

        if publisher is not None:
            return PublisherBroker(publisher, clock=clock)
        if not config.sqs_queue_url:
            raise ConfigurationError(
                "the ledger outbox drain has no durable downstream: SQS_QUEUE_URL is unset. "
                "Refusing rather than falling back to the in-process bus, which would mark "
                "every row sent after handing it to an empty subscriber list -- the rows "
                "would leave the outbox table and the events would be lost."
            )
        from shepherd_workflow_service.adapters.real.sqs_event_bus import SqsPublisher

        return PublisherBroker(
            SqsPublisher(queue_url=config.sqs_queue_url, region_name=config.aws_region),
            clock=clock,
        )

    return factory


def _build_workflow_engine(
    config: ServiceConfig, clock: Clock, secrets: SecretsPort | None
) -> WorkflowEnginePort:
    """Wire the vendored workflow package over this deployment's database.

    Both arguments this function passes down are CALLABLES. `build_container()`
    must complete with zero I/O -- `tests/unit/test_container.py` asserts it
    under a `socket.socket` that raises -- and resolving a database password
    through the secrets port and reading the workflow definition off disk are
    both I/O. They happen on the first request instead, where a failure is a 500
    with a real cause rather than a process that would not start.
    """
    from shepherd_workflow_service.adapters.real.json_workflow_definition import (
        load_workflow_definition,
    )
    from shepherd_workflow_service.adapters.real.sql_workflow_engine import SqlWorkflowEngine

    _require_a_buildable_database(config, secrets)

    path = config.workflow_definition_path
    if not path:
        raise ConfigurationError(
            "WORKFLOW_DEFINITION_PATH is not set, and this service has no built-in "
            "workflow. A default would accept jobs it can never advance: every command "
            "would be refused as workflow_digest_mismatch, which reads as a caller bug. "
            "Point it at a JSON workflow definition (see contracts/workflow.v1.json)."
        )

    return SqlWorkflowEngine(
        url_provider=lambda: _database_url(config, secrets),
        workflow_provider=lambda: load_workflow_definition(path),
        clock=clock,
        job_deadline=timedelta(seconds=config.workflow_job_deadline_seconds),
        # ONE value reaches both `build_tables(prefix=...)` and `owned_tables(...)`
        # inside the engine. See that adapter's module docstring for why a second
        # literal written at the default is the defect this estate has already paid for.
        table_prefix=config.workflow_table_prefix,
    )


def _default_secrets(config: ServiceConfig) -> SecretsPort | None:
    """The SecretsPort a deployment gets when no caller injected one.

    Returns None for every backend that needs no secret, and None when the real
    adapter cannot be constructed, so `_require_a_buildable_database` stays the single
    place that decides whether the resulting configuration is possible. Returning a
    half-built adapter here instead would move that verdict into two places and let the
    failure surface later, on the first request that touches the ledger.

    The import is function-local and the adapter connects lazily, so this stays
    network-free at build time and `test_container_builds_with_zero_network_access`
    keeps holding -- the same pattern the JWT auth branch above already uses.
    """
    if config.db_backend != "mysql":
        return None
    if not config.aws_region:
        return None
    from shepherd_workflow_service.adapters.real.aws_secrets_manager import (
        AwsSecretsManagerSecrets,
    )

    return AwsSecretsManagerSecrets(region=config.aws_region)


def _require_a_buildable_database(config: ServiceConfig, secrets: SecretsPort | None) -> None:
    """Refuse an impossible database configuration at BUILD time. No fall-through.

    An unrecognised backend raises rather than defaulting to SQLite: a green
    service writing a durable ledger to a local file nobody backs up fails as
    missing data, long after the typo
    (`knowledge/wiki/a-fall-through-inherits-the-branchs-severity.md`).

    This runs eagerly, from `_build_workflow_engine`, even though the URL itself
    is resolved lazily. The two are different questions and only one of them is
    I/O: whether the backend NAME is known, and whether a `SecretsPort` was
    supplied, are both answerable from the arguments alone, so a typo in
    `DB_BACKEND` kills the process at startup instead of surfacing as a 500 on
    the first request that touches the ledger.

    It is also the single authority `_database_url` consults, rather than a
    second copy of the same branch list. Two copies agree until the day one of
    them learns a new backend.
    """
    if config.db_backend not in _SUPPORTED_DB_BACKENDS:
        raise ConfigurationError(
            f"unknown db_backend={config.db_backend!r}; "
            f"supported backends are {_SUPPORTED_DB_BACKENDS}"
        )
    if config.db_backend == "mysql" and secrets is None:
        # The message names the ENVIRONMENT fix first, because that is the one a
        # deployment can act on. Its previous wording said only "pass secrets=<adapter>
        # to build_container()", which is actionable from a test and from nowhere else:
        # an ECS task or a Lambda has no call site to edit, so the only instruction it
        # was given was one it could not follow.
        raise ConfigurationError(
            "db_backend='mysql' needs a SecretsPort to resolve "
            f"db_password_secret_id={config.db_password_secret_id!r}, and none could be "
            "built because AWS_REGION is unset. Set AWS_REGION so the default "
            "AwsSecretsManagerSecrets adapter can be constructed, or pass "
            "secrets=<adapter> to build_container() (tests do this)."
        )


def _database_url(config: ServiceConfig, secrets: SecretsPort | None) -> str:
    """The SQLAlchemy URL for the workflow ledger, resolved on first use."""
    _require_a_buildable_database(config, secrets)

    if config.db_backend == "sqlite":
        return f"sqlite+pysqlite:///{config.db_path}"

    # MySQL. `_require_a_buildable_database` has already rejected every other
    # backend name and the missing-secrets case, so both narrowings below hold.
    assert secrets is not None
    secret_id = config.db_password_secret_id
    assert secret_id is not None  # ServiceConfig.__post_init__ already enforced this
    password = quote_plus(secrets.get(secret_id))
    user = quote_plus(config.db_user or "")
    return (
        f"mysql+pymysql://{user}:{password}@{config.db_host}:{config.db_port}/"
        f"{config.db_name}?charset=utf8mb4"
    )
