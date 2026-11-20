"""Verify authenticated tenant admission against an isolated local MySQL 8.0.46 ledger."""

import argparse
import base64
import hashlib
import json
import secrets
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from functools import partial
from ipaddress import ip_network
from pathlib import Path
from threading import Barrier
from urllib.parse import quote_from_bytes
from uuid import uuid4

import jwt
import pymysql
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.x509.oid import NameOID
from shepherd_contracts.clock import FrozenClock
from shepherd_contracts.errors import ConfigurationError, Unauthorized
from sqlalchemy import URL, select
from verification_support import cleanup_docker, file_hashes, publish_receipt
from verify_mysql import docker

from shepherd_workflow_service.adapters.real.jwt_principal_auth import JwksKeyResolver
from shepherd_workflow_service.adapters.real.sql_execution import SqlExecutionRepository
from shepherd_workflow_service.adapters.real.sql_workflow_engine import SqlWorkflowEngine
from shepherd_workflow_service.adapters.real.tenant_sql import (
    check_tenant_collations,
    migrate_tenant_ownership,
)
from shepherd_workflow_service.adapters.real.tenant_workload_auth import TenantWorkloadAuth
from shepherd_workflow_service.adapters.real.workflow_catalog import JsonWorkflowCatalog
from shepherd_workflow_service.errors import (
    ExecutionBackpressure,
    StageRunNotFound,
    StageRunRejected,
)
from shepherd_workflow_service.models.execution import EventEnvelope
from shepherd_workflow_service.models.scheduling import TenantPolicy
from shepherd_workflow_service.models.tenancy import (
    ExecutorGrant,
    TenantRegistry,
    TenantWorkload,
    TriggerGrant,
)
from shepherd_workflow_service.services.execution import ExecutionService
from shepherd_workflow_service.services.scheduling_strategy import SchedulingStrategy

ISSUER = "https://mysql-probe.example/issuer"
AUDIENCE = "https://mysql-probe.example/workflow"
PEER = "127.0.0.1"
TENANTS = ("tenant-a", "tenant-b")
PREFIX = "tenant_probe_"
LEGACY_COLLATION = "utf8mb4_unicode_ci"


def expect_error(expected, operation):
    try:
        operation()
    except expected:
        return
    raise AssertionError(f"expected {expected.__name__}")


def race(count, operation):
    start = Barrier(count)

    def synchronized(index):
        start.wait(timeout=15)
        return operation(index)

    with ThreadPoolExecutor(max_workers=count) as threads:
        return list(threads.map(synchronized, range(count)))


class Credentials:
    def __init__(self, definition, clock):
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.headers = {}
        stage = definition.stages[0]
        workloads, triggers, executors = [], [], []
        for tenant in TENANTS:
            submitter = self.register(tenant, definition.trigger.producer, "submit", clock)
            executor = self.register(tenant, stage.completion.success.producer, "complete", clock)
            workloads.extend((submitter, executor))
            triggers.append(
                TriggerGrant(
                    tenant_id=tenant,
                    producer=submitter.producer,
                    event_type=definition.trigger.event_type,
                    workflow_digest=definition.digest,
                )
            )
            executors.append(
                ExecutorGrant(
                    tenant_id=tenant,
                    issuer=ISSUER,
                    client_id=executor.client_id,
                    workflow_digest=definition.digest,
                    stage=stage.key,
                )
            )
        self.registry = TenantRegistry(
            workloads=tuple(workloads),
            triggers=tuple(triggers),
            executors=tuple(executors),
        )
        self.auth = TenantWorkloadAuth(
            key_resolver=JwksKeyResolver(f"{ISSUER}/jwks", fetch=lambda _: self.key.public_key()),
            registry=lambda: self.registry,
            issuer=ISSUER,
            audience=AUDIENCE,
            proxy_networks=[ip_network("127.0.0.1/32")],
            profile="rfc9068",
            clock=clock,
        )

    def register(self, tenant, producer, operation, clock):
        client = f"{tenant}-{operation}"
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, client)])
        now = clock.now()
        client_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        certificate = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Probe issuer")]))
            .public_key(client_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(days=1))
            .not_valid_after(now + timedelta(days=1))
            .sign(self.key, hashes.SHA256())
        )
        thumbprint = base64.urlsafe_b64encode(certificate.fingerprint(hashes.SHA256()))
        thumbprint = thumbprint.decode().rstrip("=")
        scopes = (f"workflow:{operation}", "workflow:read")
        token = jwt.encode(
            {
                "iss": ISSUER,
                "aud": AUDIENCE,
                "sub": client,
                "client_id": client,
                "scope": " ".join(scopes),
                "jti": f"jwt-{client}",
                "iat": int(now.timestamp()),
                "exp": int(now.timestamp()) + 1800,
                "cnf": {"x5t#S256": thumbprint},
            },
            self.key,
            algorithm="RS256",
            headers={"kid": "probe-key", "typ": "at+jwt"},
        )
        self.headers[tenant, operation] = [
            (b"authorization", f"Bearer {token}".encode()),
            (
                b"x-amzn-mtls-clientcert-leaf",
                quote_from_bytes(certificate.public_bytes(Encoding.PEM), safe="").encode(),
            ),
        ]
        return TenantWorkload(
            issuer=ISSUER,
            client_id=client,
            subject=client,
            tenant_id=tenant,
            producer=producer,
            active=True,
            scopes=scopes,
            certificate_thumbprints=(thumbprint,),
        )

    def principal(self, tenant, operation="submit"):
        return self.auth.authenticate(self.headers[tenant, operation], peer_ip=PEER)


def table_rows(repository, table):
    with repository._connection() as connection:
        return connection.execute(select(table)).mappings().all()


def dispatches(repository):
    return [
        row["payload"]["envelope"]
        for row in table_rows(repository, repository._base().tables["outbox"])
        if "envelope" in row["payload"] and "correlation" in row["payload"]["envelope"]
    ]


def verify_migration(engine, repository_factory, job_id):
    table = PREFIX + "jobs"
    aliases = ("Tenant-a", "ténant-a", "tenant-a ")
    with engine.open_connection() as connection:
        default_collation = connection.exec_driver_sql("SELECT @@collation_database").scalar_one()
        assert default_collation == LEGACY_COLLATION
        connection.exec_driver_sql(
            f"ALTER TABLE {table} MODIFY COLUMN org_id VARCHAR(64)"
            f" CHARACTER SET utf8mb4 COLLATE {LEGACY_COLLATION} NOT NULL"
        )
        for alias in aliases:
            assert (
                connection.exec_driver_sql(
                    f"SELECT COUNT(*) FROM {table} WHERE org_id=%s AND job_id=%s",
                    (alias, job_id),
                ).scalar_one()
                == 1
            )
        connection.commit()
    expect_error(ConfigurationError, repository_factory().check_ready)

    for alias in aliases:
        with engine.open_connection() as connection:
            connection.exec_driver_sql(
                f"UPDATE {table} SET org_id=%s WHERE job_id=%s", (alias, job_id)
            )
            connection.commit()
            expect_error(ConfigurationError, lambda: migrate_tenant_ownership(connection, PREFIX))
            expect_error(ConfigurationError, lambda: check_tenant_collations(connection, PREFIX))
            connection.exec_driver_sql(
                f"UPDATE {table} SET org_id=%s WHERE job_id=%s", (TENANTS[0], job_id)
            )
            connection.commit()

    with engine.open_connection() as connection:
        migrate_tenant_ownership(connection, PREFIX)
        connection.commit()
        check_tenant_collations(connection, PREFIX)
        for alias in aliases:
            assert (
                connection.exec_driver_sql(
                    f"SELECT COUNT(*) FROM {table} WHERE org_id=%s AND job_id=%s",
                    (alias, job_id),
                ).scalar_one()
                == 0
            )
    repository_factory().check_ready()
    return list(aliases)


def scenario(url, catalog_path, restart):
    clock = FrozenClock(datetime.now(UTC).replace(microsecond=0))
    catalog = JsonWorkflowCatalog(str(catalog_path))
    definition = catalog.load().workflows[0]
    credentials = Credentials(definition, clock)

    def repository(*, prefix=PREFIX, capacity=1, scheduled=True):
        engine = SqlWorkflowEngine(
            url_provider=url,
            workflow_provider=definition.ledger_definition,
            clock=clock,
            job_deadline=timedelta(hours=1),
            table_prefix=prefix,
        )
        return SqlExecutionRepository(
            catalog=catalog,
            engine_factory=lambda _: engine,
            clock=clock,
            prefix=prefix,
            producer="mysql-admission-probe",
            scheduler=SchedulingStrategy() if scheduled else None,
            pool_capacity=capacity,
            tenant_registry=credentials.registry,
        )

    def service(repo):
        return ExecutionService(repo, tenant_registry=credentials.registry)

    def trigger(tenant, event_id):
        return EventEnvelope(
            event_id=event_id,
            type=definition.trigger.event_type,
            producer=definition.trigger.producer,
            tenant_id=tenant,
            occurred_at=clock.now(),
        )

    def submit(repo, tenant, event_id):
        return service(repo).receive(trigger(tenant, event_id), credentials.principal(tenant))[0]

    def finish(repo, dispatch, tenant=None):
        tenant = tenant or dispatch["tenant_id"]
        success = definition.stages[0].completion.success
        completion = EventEnvelope.model_validate_json(
            json.dumps(
                {
                    "event_id": f"complete-{dispatch['correlation']['stage_run_id']}",
                    "type": success.event_type,
                    "producer": success.producer,
                    "tenant_id": tenant,
                    "occurred_at": clock.now().isoformat(),
                    "payload_digest": "probe-output-digest",
                    "correlation": dispatch["correlation"],
                }
            )
        )
        return service(repo).receive(completion, credentials.principal(tenant, "complete"))

    policies = {
        TENANTS[0]: TenantPolicy(weight=2, backlog_cap=3, in_flight_cap=1),
        TENANTS[1]: TenantPolicy(weight=1, backlog_cap=3, in_flight_cap=1),
    }
    initial = repository()
    expect_error(ConfigurationError, initial.check_ready)
    initial.reconcile_and_enable(tenant_policies=policies)
    workers = [repository() for _ in range(6)]
    jobs = race(6, lambda i: submit(workers[i], TENANTS[i % 2], f"shared-{i // 2}"))
    assert len(set(jobs)) == 6
    assert not dispatches(initial)
    for index, job in enumerate(jobs):
        owner, foreign = TENANTS[index % 2], TENANTS[(index + 1) % 2]
        stages = service(initial).stages(job, credentials.principal(owner))
        assert len(stages) == 1
        assert not service(initial).stages(job, credentials.principal(foreign))
        assert service(initial).get(stages[0].stage_run_id, credentials.principal(foreign)) is None
        expect_error(
            StageRunNotFound,
            partial(service(initial).require_job, job, credentials.principal(foreign)),
        )
    expect_error(
        Unauthorized,
        lambda: service(initial).receive(
            trigger(TENANTS[0], "forged"), credentials.principal(TENANTS[1])
        ),
    )
    wrong_cert = credentials.headers[TENANTS[1], "submit"][1]
    bearer = credentials.headers[TENANTS[0], "submit"][0]
    expect_error(
        Unauthorized, lambda: credentials.auth.authenticate([bearer, wrong_cert], peer_ip=PEER)
    )
    expect_error(ExecutionBackpressure, lambda: submit(initial, TENANTS[0], "over-backlog"))
    expect_error(
        ExecutionBackpressure, lambda: submit(repository(scheduled=False), TENANTS[0], "old-writer")
    )

    def fail_lease(*args, **kwargs):
        raise RuntimeError("injected admission rollback")

    failing = repository()
    failing.admission.record_lease = fail_lease
    expect_error(RuntimeError, lambda: failing.admit(limit=1))
    assert not dispatches(initial)
    assert not table_rows(initial, initial.admission.attempts)
    assert {row["status"] for row in table_rows(initial, initial._base().tables["stage_runs"])} == {
        "pending",
    }
    assert len(table_rows(initial, initial.admission.ready)) == 6
    assert table_rows(initial, initial.admission.pool)[0]["admissions"] == 0

    assert sum(race(6, lambda i: workers[i].admit(limit=1))) == 1
    (first,) = dispatches(initial)
    assert first["tenant_id"] == TENANTS[0] and first["correlation"]["fence"] == 1
    (state,) = table_rows(initial, initial.admission.pool)
    assert state["cursor"] == TENANTS[0] and state["visit_open"] and state["admissions"] == 1
    restart()
    fresh = repository()
    assert fresh.admit(limit=1) == 0
    expect_error(StageRunRejected, lambda: finish(fresh, first, tenant=TENANTS[1]))
    assert finish(fresh, first) == finish(fresh, first)
    assert [row["samples"] for row in table_rows(fresh, fresh.admission.estimates)] == [1]
    assert fresh.admit(limit=1) == 1
    (second,) = [event for event in dispatches(fresh) if event != first]
    assert second["tenant_id"] == TENANTS[0]
    assert finish(fresh, second)
    assert fresh.admit(limit=1) == 1
    (third,) = [event for event in dispatches(fresh) if event not in (first, second)]
    assert third["tenant_id"] == TENANTS[1]

    cap_workers = [repository(prefix="tenant_cap_probe_", capacity=2) for _ in range(4)]
    cap_repository = cap_workers[0]
    cap_repository.reconcile_and_enable(tenant_policies=policies)
    for index in range(2):
        submit(cap_repository, TENANTS[0], f"tenant-cap-{index}")
    assert sum(race(4, lambda i: cap_workers[i].admit(limit=1))) == 1
    submit(cap_repository, TENANTS[1], "borrowed-slot")
    assert sum(race(4, lambda i: cap_workers[i].admit(limit=1))) == 1
    assert {event["tenant_id"] for event in dispatches(cap_repository)} == set(TENANTS)

    formerly_valid = credentials.principal(TENANTS[0])
    revoked = credentials.registry.workloads[0].model_copy(update={"active": False})
    credentials.registry = credentials.registry.model_copy(
        update={
            "workloads": (revoked, *credentials.registry.workloads[1:]),
        }
    )
    expect_error(Unauthorized, lambda: credentials.principal(TENANTS[0]))
    expect_error(
        Unauthorized,
        lambda: service(repository()).receive(trigger(TENANTS[0], "shared-0"), formerly_valid),
    )
    assert submit(repository(), TENANTS[1], "shared-0") == jobs[1]
    aliases = verify_migration(fresh._base(), repository, jobs[0])
    return {
        "workflow_key": definition.workflow_key,
        "workflow_definition_digest": definition.digest,
        "checks": [
            "signed-certificate-bound-workload-admission-race",
            "tenant-job-stage-and-event-isolation",
            "pool-tenant-and-backlog-caps",
            "legacy-writer-cutover-guard",
            "transactional-admission-outbox-rollback",
            "weighted-cursor-and-credit-survive-mysql-restart",
            "result-idempotency",
            "revoked-credential-and-receipt-replay-rejected",
            "unsafe-collation-readiness-guard-and-migration",
        ],
        "ownership_aliases_rejected": aliases,
        "legacy_database_collation": LEGACY_COLLATION,
        "admission_sequence": [event["tenant_id"] for event in (first, second, third)],
    }


def main():
    if not __debug__:
        raise RuntimeError("verification requires Python assertions enabled")
    project = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="mysql:8.0.46")
    parser.add_argument(
        "--catalog", type=Path, default=project / "contracts/examples/catalog.v2.json"
    )
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    if args.receipt.exists():
        parser.error("receipt exists; choose a fresh path")
    runtime_paths = []
    for folder in ("src", "vendor/shepherd_workflow", "vendor/shepherd_contracts"):
        runtime_paths.extend(sorted((project / folder).rglob("*.py")))
    starting_hashes = file_hashes(
        [
            Path(__file__).resolve(),
            Path(__file__).with_name("verification_support.py").resolve(),
            Path(__file__).with_name("verify_mysql.py").resolve(),
            args.catalog.resolve(),
            *runtime_paths,
        ]
    )
    runtime_manifest = {
        str(path.relative_to(project)): starting_hashes[str(path)] for path in runtime_paths
    }
    catalog_bytes = args.catalog.read_bytes()
    image_id = json.loads(docker("image", "inspect", args.image))[0]["Id"]
    name = f"wf-probe-tenant-mysql-{uuid4().hex[:10]}"
    volume = name + "-data"
    password = secrets.token_hex(24)
    port = 0

    def ready():
        nonlocal port
        port = int(docker("port", name, "3306/tcp").split(":")[-1])
        for _ in range(90):
            try:
                with (
                    pymysql.connect(
                        host=PEER,
                        port=port,
                        user="probe",
                        password=password,
                        database="workflow_probe",
                        connect_timeout=2,
                    ) as connection,
                    connection.cursor() as cursor,
                ):
                    cursor.execute("SELECT VERSION()")
                    version = cursor.fetchone()[0]
                    assert version.startswith("8.0.46"), version
                    return version
            except pymysql.MySQLError:
                time.sleep(1)
        raise RuntimeError("isolated MySQL readiness deadline exceeded")

    def url():
        return URL.create(
            "mysql+pymysql",
            username="probe",
            password=password,
            host=PEER,
            port=port,
            database="workflow_probe",
        ).render_as_string(hide_password=False)

    def restart():
        docker("restart", name)
        ready()

    try:
        docker("volume", "create", volume)
        with tempfile.TemporaryDirectory(prefix=name) as work:
            snapshot = Path(work) / "catalog.json"
            snapshot.write_bytes(catalog_bytes)
            env = Path(work) / "mysql.env"
            env.touch(mode=0o600)
            env.write_text(
                f"MYSQL_DATABASE=workflow_probe\nMYSQL_USER=probe\nMYSQL_PASSWORD={password}\n"
                f"MYSQL_ROOT_PASSWORD={secrets.token_hex(24)}\n"
            )
            docker(
                "run",
                "-d",
                "--pull=never",
                "--name",
                name,
                "--memory=768m",
                "--cpus=1",
                "--pids-limit=256",
                "--env-file",
                str(env),
                "-p",
                "127.0.0.1::3306",
                "-v",
                f"{volume}:/var/lib/mysql",
                image_id,
                "--character-set-server=utf8mb4",
                f"--collation-server={LEGACY_COLLATION}",
            )
            version = ready()
            evidence = scenario(url, snapshot, restart)
    finally:
        cleanup = cleanup_docker(containers=[name], volumes=[volume])
    evidence.update(
        scope="isolated-local-mysql-tenant-admission",
        mysql_version=version,
        image_id=image_id,
        catalog_sha256=hashlib.sha256(catalog_bytes).hexdigest(),
        probe_sha256=starting_hashes[str(Path(__file__).resolve())],
        runtime_sources_sha256=hashlib.sha256(
            json.dumps(runtime_manifest, sort_keys=True).encode()
        ).hexdigest(),
        runtime_sources=runtime_manifest,
        cleanup=cleanup,
    )
    publish_receipt(args.receipt, evidence, starting_hashes)
    print(json.dumps({"mysql_version": version, "receipt": str(args.receipt)}))


if __name__ == "__main__":
    main()
