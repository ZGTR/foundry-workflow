"""Durable recovery contract through real SQL and a restarted HTTP process."""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from shepherd_contracts.clock import FrozenClock
from shepherd_workflow.workflow import StageDefinitionV1, WorkflowDefinitionV1

from shepherd_workflow_service.adapters.real.sql_workflow_engine import SqlWorkflowEngine
from shepherd_workflow_service.errors import StageRunConflict
from tests.integration.test_sql_workflow_engine import _principal

ROOT = Path(__file__).resolve().parents[2]


def _engine(tmp_path, clock, workflow=None):
    workflow = workflow or WorkflowDefinitionV1(
        "test.v1",
        (
            StageDefinitionV1("first", "a", "b", max_attempts=2, retry_delay_seconds=30),
            StageDefinitionV1("second", "b", "c"),
        ),
    )
    return SqlWorkflowEngine(
        url_provider=lambda: f"sqlite+pysqlite:///{tmp_path / 'ledger.db'}",
        workflow_provider=lambda: workflow,
        clock=clock,
        job_deadline=timedelta(hours=1),
        table_prefix="wf_",
    )


def _command(engine, view, kind, **payload):
    return engine.submit(
        stage_run_id=view.stage_run_id,
        kind=kind,
        state_version=view.state_version,
        fence=view.fence,
        payload=payload,
        principal=_principal(),
    )


def test_retry_due_and_attempt_limit_survive_new_engine(tmp_path):
    clock = FrozenClock(datetime(2026, 10, 18, tzinfo=UTC))
    engine = _engine(tmp_path, clock)
    start = engine.start_job(job_id="j", input_digest="input", principal=_principal())
    lease = _command(engine, start, "lease").view
    assert _command(engine, lease, "lease").refusal == "lease_held"
    failed = _command(engine, lease, "fail", retryable=True).view
    assert failed.status == "retry_wait"
    assert failed.next_attempt_at == clock.now() + timedelta(seconds=30)
    restarted = _engine(tmp_path, clock)
    assert _command(restarted, failed, "lease").refusal == "retry_not_due"
    clock.advance(30)
    lease2 = _command(restarted, failed, "lease").view
    assert lease2.fence == 2
    exhausted = _command(restarted, lease2, "fail", retryable=True).view
    assert exhausted.status == "failed"
    assert _command(restarted, exhausted, "lease").refusal == "terminal_state"


def test_expired_lease_reclaim_fences_old_worker_and_preserves_output(tmp_path):
    clock = FrozenClock(datetime(2026, 10, 18, tzinfo=UTC))
    engine = _engine(tmp_path, clock)
    start = engine.start_job(job_id="j", input_digest="input", principal=_principal())
    lease = _command(engine, start, "lease").view
    clock.advance(301)
    reclaimed = _command(engine, lease, "lease").view
    assert reclaimed.fence == 2
    stale = engine.submit(
        stage_run_id=lease.stage_run_id,
        kind="complete",
        state_version=reclaimed.state_version,
        fence=lease.fence,
        payload={"payload_digest": "wrong"},
        principal=_principal(),
    )
    assert stale.refusal == "stale_fence"
    complete = _command(
        engine, reclaimed, "complete", payload_digest="digest", output_ref="sha256:digest"
    ).view
    assert complete.artifact_digest and complete.output_ref == "sha256:digest"
    assert _command(engine, complete, "lease").refusal == "terminal_state"
    restarted = _engine(tmp_path, clock)
    stages = restarted.stages("j", principal=_principal())
    assert [view.stage for view in stages] == ["first", "second"]
    assert stages[1].stage_run_id == complete.next_stage_run_id
    assert stages[1].input_digest == complete.artifact_digest
    assert restarted.get(complete.stage_run_id, principal=_principal()) == complete


def test_duplicate_start_cannot_disclose_or_attach_another_tenant(tmp_path):
    clock = FrozenClock(datetime(2026, 10, 18, tzinfo=UTC))
    engine = _engine(tmp_path, clock)
    first = engine.start_job(job_id="j", input_digest="input", principal=_principal())
    for digest, principal in [("input", _principal("other")), ("changed", _principal())]:
        with pytest.raises(StageRunConflict, match="job_identity_conflict"):
            engine.start_job(job_id="j", input_digest=digest, principal=principal)
    assert engine.start_job(job_id="j", input_digest="input", principal=_principal()) == first
    assert engine.stages("j", principal=_principal("other")) == []


def test_legacy_definition_digest_unchanged():
    from shepherd_contracts import canonical_digest

    workflow = WorkflowDefinitionV1("legacy", (StageDefinitionV1("a", "b", "c"),))
    assert workflow.digest == canonical_digest(
        {
            "workflow_key": "legacy",
            "stages": [
                {"key": "a", "input_schema": "b", "output_schema": "c"},
            ],
        }
    )


@contextmanager
def _http_process(tmp_path):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {
        **os.environ,
        "PYTHONPATH": f"{ROOT}:{ROOT / 'src'}:{ROOT / 'vendor'}",
        "PORT": str(port),
        "DB_PATH": str(tmp_path / "http.db"),
        "SERVICE_ENV": "local",
        "WORKFLOW_DEFINITION_PATH": str(ROOT / "contracts/learning-printables.v1.json"),
    }
    with (tmp_path / "http.log").open("a") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "shepherd_workflow_service.main"],
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=log,
        )
        try:
            with httpx.Client(
                base_url=f"http://127.0.0.1:{port}",
                headers={
                    "x-org-id": "org_test",
                    "x-subject-id": "local",
                    "x-scopes": "jobs:write",
                },
                timeout=5,
            ) as client:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if proc.poll() is not None:
                        pytest.fail((tmp_path / "http.log").read_text())
                    try:
                        if client.get("/healthz").status_code == 200:
                            break
                    except httpx.ConnectError:
                        pass
                    time.sleep(0.05)
                else:
                    pytest.fail("HTTP service did not become healthy within 30s")
                yield client
        finally:
            proc.terminate()
            proc.wait(timeout=10)


def test_real_http_restart_retains_retry_outputs_successors_and_outbox(tmp_path):
    def command(client, view, kind, payload=None):
        return client.post(
            f"/v1/stage-runs/{view['stage_run_id']}/commands",
            json={
                "kind": kind,
                "state_version": view["state_version"],
                "fence": view["fence"],
                "payload": payload or {},
            },
        )

    with _http_process(tmp_path) as client:
        first = client.post("/v1/jobs", json={"job_id": "http-job", "input_digest": "in"})
        assert first.status_code == 201
        view = first.json()
        lease = command(client, view, "lease").json()
        assert command(client, lease, "lease").status_code == 409
        retry = command(client, lease, "fail", {"retryable": True, "reason": "temporary"}).json()
        assert retry["status"] == "retry_wait" and retry["next_attempt_at"]
    with sqlite3.connect(tmp_path / "http.db") as db:
        assert db.execute("select next_attempt_at from wf_stage_runs").fetchone()[0]
        assert db.execute("select count(*) from wf_outbox where sent_at is null").fetchone()[0] == 2
    time.sleep(1.05)
    with _http_process(tmp_path) as client:
        stages = client.get("/v1/jobs/http-job/stages").json()
        assert stages == [retry]
        current = stages[0]
        completed = []
        for expected in ("normalize", "compile", "render", "verify"):
            assert current["stage"] == expected
            leased = command(client, current, "lease")
            assert leased.status_code == 200, leased.text
            response = command(
                client,
                leased.json(),
                "complete",
                {
                    "payload_digest": expected,
                    "output_ref": "sha256:" + expected,
                },
            )
            assert response.status_code == 200, response.text
            done = response.json()
            completed.append(done)
            assert (
                command(client, done, "complete", {"payload_digest": "changed"}).status_code == 409
            )
            if done["next_stage_run_id"]:
                current = client.get("/v1/stage-runs/" + done["next_stage_run_id"]).json()
        assert done["status"] == "review_wait"
        assert client.get("/v1/jobs/http-job/stages").json() == completed
        assert (
            client.get("/v1/jobs/http-job/stages", headers={"x-org-id": "other"}).status_code == 404
        )
        assert (
            client.post(
                "/v1/jobs",
                json={"job_id": "http-job", "input_digest": "in"},
                headers={"x-org-id": "other"},
            ).status_code
            == 409
        )
    with _http_process(tmp_path) as client:
        assert client.get("/v1/jobs/http-job/stages").json() == completed
    with sqlite3.connect(tmp_path / "http.db") as db:
        rows = db.execute(
            "select payload from wf_outbox where event_class='stage.succeeded'"
        ).fetchall()
        assert len(rows) == 4
        assert all(json.loads(row[0])["artifact_digest"] for row in rows)


def test_legacy_sqlite_requires_explicit_idempotent_migration(tmp_path):
    from shepherd_contracts.errors import ConfigurationError

    from scripts.migrate_recovery_sqlite import migrate

    clock = FrozenClock(datetime(2026, 10, 18, tzinfo=UTC))
    engine = _engine(tmp_path, clock)
    original = engine.start_job(job_id="legacy", input_digest="in", principal=_principal())
    with sqlite3.connect(tmp_path / "ledger.db") as db:
        for column in ("next_attempt_at", "artifact_digest", "output_ref", "next_stage_run_id"):
            db.execute(f"ALTER TABLE wf_stage_runs DROP COLUMN {column}")
    with pytest.raises(ConfigurationError, match="migration required"):
        _engine(tmp_path, clock).get(original.stage_run_id, principal=_principal())
    migrate(tmp_path / "ledger.db", "wf_")
    migrate(tmp_path / "ledger.db", "wf_")
    assert _engine(tmp_path, clock).get(original.stage_run_id, principal=_principal()) == original


def test_invalid_completion_reference_is_validation_error():
    from pydantic import ValidationError

    from shepherd_workflow_service.schemas.workflow import CommandRequest

    with pytest.raises(ValidationError, match="output_ref"):
        CommandRequest(kind="complete", state_version=1, payload={"output_ref": {"bad": "type"}})
