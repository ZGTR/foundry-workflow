"""Verify the real ledger against an isolated local MySQL image, without cloud access."""

import argparse
import hashlib
import json
import secrets
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pymysql
from shepherd_contracts.clock import FrozenClock
from shepherd_contracts.principal import PrincipalMinter
from sqlalchemy import URL
from verification_support import cleanup_docker, file_hashes, publish_receipt

from shepherd_workflow_service.adapters.real.json_workflow_definition import (
    load_workflow_definition,
)
from shepherd_workflow_service.adapters.real.sql_workflow_engine import SqlWorkflowEngine


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def main():
    if not __debug__:
        raise RuntimeError("verification requires Python assertions enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="Existing local MySQL image; never pulled")
    parser.add_argument("--definition", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    if args.receipt.exists():
        parser.error("receipt exists; choose a fresh path")
    project = Path(__file__).resolve().parents[2]
    sources = [
        Path(__file__).resolve(),
        Path(__file__).with_name("verification_support.py").resolve(),
    ]
    for folder in ("src", "vendor/shepherd_workflow", "vendor/shepherd_contracts"):
        sources.extend(sorted((project / folder).rglob("*.py")))
    starting_hashes = file_hashes(sources)
    runtime_manifest = {
        str(Path(path).relative_to(project)): digest for path, digest in starting_hashes.items()
    }
    image_id = json.loads(docker("image", "inspect", args.image))[0]["Id"]
    definition_bytes = args.definition.read_bytes()
    with tempfile.NamedTemporaryFile() as snapshot:
        snapshot.write(definition_bytes)
        snapshot.flush()
        definition = load_workflow_definition(snapshot.name)
    name = "wf-probe-mysql-" + uuid4().hex[:10]
    volume = name + "-data"
    password = secrets.token_hex(24)
    principal = PrincipalMinter(issuer="mysql-probe").mint(
        org_id="example-org", subject_id="probe", scopes=frozenset({"jobs:write", "jobs:read"})
    )
    port = 0

    def connect():
        return pymysql.connect(
            host="127.0.0.1",
            port=port,
            user="probe",
            password=password,
            database="workflow_probe",
            connect_timeout=2,
        )

    def ready():
        nonlocal port
        port = int(docker("port", name, "3306/tcp").split(":")[-1])
        for _ in range(90):
            try:
                with connect() as connection, connection.cursor() as cursor:
                    cursor.execute("SELECT VERSION()")
                    return cursor.fetchone()[0]
            except pymysql.MySQLError:
                time.sleep(1)
        raise RuntimeError("isolated MySQL readiness deadline exceeded")

    def engine():
        return SqlWorkflowEngine(
            url_provider=lambda: URL.create(
                "mysql+pymysql",
                username="probe",
                password=password,
                host="127.0.0.1",
                port=port,
                database="workflow_probe",
            ).render_as_string(hide_password=False),
            workflow_provider=lambda: definition,
            clock=FrozenClock(datetime.now(UTC)),
            job_deadline=timedelta(hours=1),
            table_prefix="printable_mysql_",
        )

    def command(ledger, view, kind, payload=None, fence=None):
        return ledger.submit(
            stage_run_id=view.stage_run_id,
            kind=kind,
            state_version=view.state_version,
            fence=view.fence if fence is None else fence,
            payload=payload or {},
            principal=principal,
        )

    try:
        docker("volume", "create", volume)
        with tempfile.TemporaryDirectory(prefix=name) as temporary:
            envfile = Path(temporary) / "mysql.env"
            envfile.touch(mode=0o600)
            envfile.write_text(
                "MYSQL_DATABASE=workflow_probe\nMYSQL_USER=probe\nMYSQL_PASSWORD="
                + password
                + "\nMYSQL_ROOT_PASSWORD="
                + secrets.token_hex(24)
                + "\n"
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
                str(envfile),
                "-p",
                "127.0.0.1::3306",
                "-v",
                volume + ":/var/lib/mysql",
                image_id,
            )
        version = ready()
        ledger = engine()
        first = ledger.start_job(
            job_id="mysql-durable", input_digest="source-digest", principal=principal
        )
        assert (
            ledger.start_job(
                job_id="mysql-durable", input_digest="source-digest", principal=principal
            )
            == first
        )
        other = PrincipalMinter(issuer="mysql-probe").mint(
            org_id="other", subject_id="probe", scopes=frozenset({"jobs:read"})
        )
        assert ledger.get(first.stage_run_id, principal=other) is None
        barrier = Barrier(2)

        def claim():
            barrier.wait(timeout=10)
            return command(ledger, first, "lease")

        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes = list(pool.map(lambda _: claim(), range(2)))
        assert sum(result.accepted for result in outcomes) == 1
        assert (
            next(result for result in outcomes if not result.accepted).refusal
            == "state_version_conflict"
        )
        leased = next(result.view for result in outcomes if result.accepted)
        assert leased is not None and leased.fence is not None
        refused = command(
            ledger, leased, "complete", {"payload_digest": "bad"}, fence=leased.fence + 1
        )
        assert not refused.accepted and refused.refusal == "stale_fence"
        docker("restart", name)
        ready()
        ledger = engine()
        assert ledger.get(first.stage_run_id, principal=principal) == leased
        completed = command(ledger, leased, "complete", {"payload_digest": "artifact-source"})
        assert completed.accepted and completed.view is not None
        assert not command(
            ledger, leased, "complete", {"payload_digest": "artifact-source"}
        ).accepted
        current = completed.view
        retry_checked = False
        while current.next_stage_run_id:
            pending = ledger.get(current.next_stage_run_id, principal=principal)
            assert pending is not None
            claimed = command(ledger, pending, "lease")
            assert claimed.accepted and claimed.view is not None
            stage_definition = definition.stage(pending.stage)
            assert stage_definition is not None
            if not retry_checked and stage_definition.max_attempts > 1:
                assert 0 < stage_definition.retry_delay_seconds <= 5
                failed = command(
                    ledger, claimed.view, "fail", {"retryable": True, "reason": "probe"}
                )
                assert failed.accepted and failed.view is not None
                assert failed.view.status == "retry_wait"
                early = command(ledger, failed.view, "lease")
                assert not early.accepted and early.refusal == "retry_not_due"
                time.sleep(stage_definition.retry_delay_seconds + 0.1)
                ledger = engine()
                retained = ledger.get(pending.stage_run_id, principal=principal)
                assert retained == failed.view
                retried = command(ledger, retained, "lease")
                assert retried.accepted and retried.view is not None
                assert retried.view.fence == claimed.view.fence + 1
                claimed = retried
                retry_checked = True
            outcome = command(
                ledger, claimed.view, "complete", {"payload_digest": "artifact-" + pending.stage}
            )
            assert outcome.accepted and outcome.view is not None
            current = outcome.view
        before = ledger.stages("mysql-durable", principal=principal)
        assert len(before) == len(definition.stages)
        assert retry_checked
        assert [stage.status for stage in before] == ["succeeded"] * (len(before) - 1) + [
            "review_wait"
        ]
        docker("restart", name)
        ready()
        assert engine().stages("mysql-durable", principal=principal) == before
        ledger = engine()
        limited = ledger.start_job(
            job_id="bounded-source", input_digest="bounded", principal=principal
        )
        limited_lease = command(ledger, limited, "lease")
        assert limited_lease.accepted and limited_lease.view is not None
        exhausted = command(
            ledger, limited_lease.view, "fail", {"retryable": True, "reason": "probe"}
        )
        assert exhausted.accepted and exhausted.view is not None
        assert exhausted.view.status == "failed"
        assert not command(ledger, exhausted.view, "lease").accepted
        evidence = {
            "scope": "local-mysql-ledger-only",
            "image_id": image_id,
            "mysql_version": version,
            "definition_sha256": hashlib.sha256(definition_bytes).hexdigest(),
            "probe_sha256": starting_hashes[str(Path(__file__).resolve())],
            "runtime_sources_sha256": hashlib.sha256(
                json.dumps(runtime_manifest, sort_keys=True).encode()
            ).hexdigest(),
            "runtime_sources": runtime_manifest,
            "checks": [
                "duplicate-start",
                "tenant-isolation",
                "concurrent-claim-one-winner",
                "stale-fence",
                "lease-survives-mysql-restart",
                "duplicate-completion",
                "retry-delay-and-fence-survive-new-engine",
                "single-attempt-source-refuses-retry",
                "all-stages-finish-at-review-wait",
                "completed-ledger-survives-mysql-restart",
            ],
            "stages": [
                {
                    "stage": view.stage,
                    "stage_run_id": view.stage_run_id,
                    "status": view.status,
                    "state_version": view.state_version,
                    "artifact_digest": view.artifact_digest,
                }
                for view in before
            ],
        }
    finally:
        cleanup = cleanup_docker(containers=[name], volumes=[volume])
    evidence["cleanup"] = cleanup
    publish_receipt(args.receipt, evidence, starting_hashes)
    print(
        json.dumps({"mysql_version": version, "stages": len(before), "receipt": str(args.receipt)})
    )


if __name__ == "__main__":
    main()
