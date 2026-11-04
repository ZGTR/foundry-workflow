"""The scheduled entrypoint, run as a REAL PROCESS against a REAL queue endpoint.

Every other test in this repository imports the drain. This one runs
`python -m shepherd_workflow_service.ledger_drain_main` the way a scheduled
runner would, so what is proved is the thing that actually gets scheduled:
`ServiceConfig.from_environment()` reading the deployment's variables,
`build_container()` resolving the broker, `boto3` signing and sending a real
SendMessage over HTTP, and the ledger marking the row afterwards.

The queue is a stub HTTP server rather than a mock inside the process, because a
mock cannot be reached across a process boundary -- and a proof that has to run
in-process to work is a proof about imports, not about the entrypoint.

THE CONTROL IS THE POINT. `test_the_row_stays_pending_when_the_entrypoint_is_not_run`
sets up the identical ledger and does not run the process. If it went green
alongside the arm above without the process doing anything, the pair could not
tell "the drain worked" from "the row was never pending in the first place".
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
from shepherd_contracts.clock import FrozenClock
from shepherd_contracts.principal import PrincipalMinter
from shepherd_workflow.sql.schema import build_tables
from sqlalchemy import MetaData, create_engine, select

from shepherd_workflow_service.adapters.real.json_workflow_definition import (
    load_workflow_definition,
)
from shepherd_workflow_service.adapters.real.sql_workflow_engine import SqlWorkflowEngine
from tests.conftest import WORKFLOW_DEFINITION_PATH

NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
PREFIX = "wf_"
REPO_ROOT = Path(__file__).resolve().parent.parent.parent


class _SqsHandler(BaseHTTPRequestHandler):
    """Answers SendMessage well enough that botocore accepts the response.

    The MD5 is computed from the body actually received: botocore verifies it,
    so a stub that returned a constant would make every send fail and the test
    would prove the retry path instead of the send path.
    """

    received: list[dict] = []

    def do_POST(self) -> None:  # noqa: N802 -- BaseHTTPRequestHandler's spelling
        length = int(self.headers.get("Content-Length", "0"))
        request = json.loads(self.rfile.read(length) or b"{}")
        type(self).received.append(request)
        body = request.get("MessageBody", "")
        payload = json.dumps(
            {
                "MD5OfMessageBody": hashlib.md5(body.encode()).hexdigest(),  # noqa: S324
                "MessageId": "msg-1",
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/x-amz-json-1.0")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args: object) -> None:
        return


@pytest.fixture
def sqs_stub():
    _SqsHandler.received = []
    server = HTTPServer(("127.0.0.1", 0), _SqsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


def _seed_one_pending_row(db_dir: Path) -> None:
    """Produce the row the way production does: a real accepted command."""
    db_dir.mkdir(parents=True, exist_ok=True)
    engine = SqlWorkflowEngine(
        url_provider=lambda: f"sqlite+pysqlite:///{db_dir / 'workflow.db'}",
        workflow_provider=lambda: load_workflow_definition(WORKFLOW_DEFINITION_PATH),
        clock=FrozenClock(NOW),
        job_deadline=timedelta(hours=1),
        table_prefix=PREFIX,
    )
    principal = PrincipalMinter(issuer="test").mint(
        org_id="org_test", subject_id="sub_1", scopes=frozenset({"jobs:write"})
    )
    view = engine.start_job(job_id="job_1", input_digest="dig_1", principal=principal)
    outcome = engine.submit(
        stage_run_id=view.stage_run_id,
        kind="lease",
        state_version=0,
        fence=None,
        payload={},
        principal=principal,
    )
    assert outcome.accepted, outcome.refusal


def _pending_keys(db_dir: Path) -> list[str]:
    tables = build_tables(MetaData(), prefix=PREFIX)
    sql_engine = create_engine(f"sqlite+pysqlite:///{db_dir / 'workflow.db'}", future=True)
    outbox = tables["outbox"]
    with sql_engine.connect() as connection:
        return [
            row[0]
            for row in connection.execute(
                select(outbox.c.key).where(outbox.c.sent_at.is_(None))
            ).all()
        ]


def _run_entrypoint(
    db_dir: Path, endpoint: str, *, queue_url: str | None = None
) -> subprocess.CompletedProcess:
    """`queue_url=None` means the variable is UNSET, not empty -- an empty string
    and an absent variable reach `ServiceConfig` differently and only one of them
    is the deployment mistake this refusal is for."""
    env = {
        **os.environ,
        "PYTHONPATH": f"{REPO_ROOT / 'src'}{os.pathsep}{REPO_ROOT / 'vendor'}",
        "SERVICE_ENV": "local",
        "DB_BACKEND": "sqlite",
        "DB_PATH": str(db_dir / "workflow.db"),
        "WORKFLOW_DEFINITION_PATH": WORKFLOW_DEFINITION_PATH,
        "WORKFLOW_TABLE_PREFIX": PREFIX,
        "AWS_REGION": "eu-west-1",
        "AWS_DEFAULT_REGION": "eu-west-1",
        "AWS_ACCESS_KEY_ID": "AKIAtest",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_ENDPOINT_URL_SQS": endpoint,
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    env.pop("SQS_QUEUE_URL", None)
    if queue_url is not None:
        env["SQS_QUEUE_URL"] = queue_url
    return subprocess.run(
        [sys.executable, "-m", "shepherd_workflow_service.ledger_drain_main"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=str(REPO_ROOT),
    )


def test_running_the_entrypoint_moves_the_row_out_of_the_outbox(
    tmp_path: Path, sqs_stub: HTTPServer
) -> None:
    _seed_one_pending_row(tmp_path)
    before = _pending_keys(tmp_path)
    assert len(before) == 1, "the fixture produced no pending row, so nothing below is measured"
    host, port = sqs_stub.server_address[0], sqs_stub.server_address[1]

    endpoint = f"http://{host}:{port}"
    result = _run_entrypoint(
        tmp_path, endpoint, queue_url=f"{endpoint}/123456789012/workflow-events"
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "drain": "ledger_outbox",
        "published": 1,
        "ambiguous": 0,
        "failed": 0,
        "total": 1,
    }
    # The queue actually received it, and it carries the stable key.
    assert len(_SqsHandler.received) == 1
    delivered = json.loads(_SqsHandler.received[0]["MessageBody"])
    assert delivered["event_type"] == "stage.leased"
    assert delivered["payload"]["key"] == before[0]
    # And the ledger no longer holds it as pending.
    assert _pending_keys(tmp_path) == []


def test_the_row_stays_pending_when_the_entrypoint_is_not_run(tmp_path: Path) -> None:
    """The control. Identical ledger, no process -- the row is still there.

    This is the state this service was in for its whole life before the drain
    existed, and it is what makes the arm above a measurement rather than an
    assertion about a row that was never pending.
    """
    _seed_one_pending_row(tmp_path)

    assert len(_pending_keys(tmp_path)) == 1


def test_the_entrypoint_refuses_when_no_queue_is_configured(tmp_path: Path) -> None:
    """A drain with nowhere durable to publish must fail loudly. The failure it
    replaces would have marked every row sent and reported a clean run."""
    _seed_one_pending_row(tmp_path)

    result = _run_entrypoint(tmp_path, "http://127.0.0.1:1", queue_url=None)

    assert result.returncode != 0
    assert "SQS_QUEUE_URL" in result.stderr
    assert len(_pending_keys(tmp_path)) == 1


def test_a_queue_that_cannot_be_reached_leaves_the_row_pending_and_exits_non_zero(
    tmp_path: Path,
) -> None:
    """The other half of the refusal: configured but unreachable. The row must
    survive, and the schedule must be able to see that something went wrong --
    a drain whose failures exit 0 is a drain nobody looks at."""
    _seed_one_pending_row(tmp_path)

    result = _run_entrypoint(
        tmp_path,
        "http://127.0.0.1:1",
        queue_url="http://127.0.0.1:1/123456789012/workflow-events",
    )

    assert result.returncode == 1
    assert json.loads(result.stdout)["failed"] == 1
    assert json.loads(result.stdout)["published"] == 0
    assert len(_pending_keys(tmp_path)) == 1
