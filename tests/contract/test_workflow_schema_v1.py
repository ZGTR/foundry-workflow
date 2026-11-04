"""The published workflow-ledger wire contract, checked against what the service
actually sends.

A schema file nobody validates is documentation: it drifts silently, and the
first reader to trust it discovers the divergence in production. This test makes
`blackboxes/workflow-ledger/schema/*.json` a contract instead of a description.

It carries controls in BOTH directions. A validator that accepted everything
would pass every green arm here, so two arms assert a REJECTION -- a response
missing a required field, and a request carrying an unknown one. Without them
the green arms prove only that the schema loaded, not that it constrains
anything.

The engine under test is the SQL one against a temporary SQLite file, not the
in-memory fake. The fake is faithful about decisions, but the translations this
file asserts -- a refusal becoming a 409 with the class in `detail`, an unknown
command kind becoming a 422 with a string `detail`, a malformed body becoming a
422 whose `detail` is a LIST -- sit between the engine and HTTP, and only a real
request exercises them.
"""

from __future__ import annotations

import json
import pathlib
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, ValidationError
from shepherd_contracts.clock import FrozenClock

from shepherd_workflow_service.app_factory import create_app
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container
from tests.conftest import WORKFLOW_DEFINITION_PATH

SCHEMA_DIR = (
    pathlib.Path(__file__).resolve().parents[2] / "blackboxes" / "workflow-ledger" / "schema"
)
STAGE_RUN_SCHEMA = SCHEMA_DIR / "stage_run.v1.json"
COMMAND_SCHEMA = SCHEMA_DIR / "command.v1.json"

HEADERS = {
    "x-org-id": "org_1",
    "x-subject-id": "user_1",
    "x-scopes": "jobs:write,jobs:read",
}
OTHER_ORG = {**HEADERS, "x-org-id": "org_2"}


def _load(path: pathlib.Path) -> dict:
    # A REFUSAL, not a skip: a missing schema file would otherwise turn this
    # whole module into a green no-op, which is what a broken contract test
    # looks like from the outside.
    assert path.is_file(), f"published schema is missing: {path}"
    return json.loads(path.read_text())


@pytest.fixture(scope="module")
def stage_run_schema() -> dict:
    return _load(STAGE_RUN_SCHEMA)


@pytest.fixture(scope="module")
def command_schema() -> dict:
    return _load(COMMAND_SCHEMA)


@pytest.fixture
def client(tmp_path: pathlib.Path) -> TestClient:
    config = ServiceConfig(
        env="test",
        port=8080,
        runtime_mode="http",
        issuer="test-issuer",
        db_path=str(tmp_path / "workflow.db"),
        event_bus_mode="in_process",
        sqs_queue_url=None,
        aws_region=None,
        workflow_definition_path=WORKFLOW_DEFINITION_PATH,
    )
    container = build_container(config, clock=FrozenClock(datetime(2026, 9, 5, tzinfo=UTC)))
    return TestClient(create_app(container))


def _start(client: TestClient) -> dict:
    response = client.post(
        "/v1/jobs", json={"job_id": "job_1", "input_digest": "dig_1"}, headers=HEADERS
    )
    assert response.status_code == 201, response.text
    return response.json()


# --- the schemas themselves -------------------------------------------------


def test_both_published_schemas_are_valid_json_schema_2020_12(
    stage_run_schema: dict, command_schema: dict
) -> None:
    Draft202012Validator.check_schema(stage_run_schema)
    Draft202012Validator.check_schema(command_schema)


def test_the_published_command_kinds_are_exactly_the_engines_own(command_schema: dict) -> None:
    """The enum is a copy, so something has to keep it honest.

    A `kind` the package adds and this file does not list would be served
    happily and published as impossible.
    """
    from shepherd_workflow.commands import CommandKind

    published = set(command_schema["properties"]["request"]["properties"]["kind"]["enum"])
    assert published == {kind.value for kind in CommandKind}


def test_the_published_refusal_classes_are_exactly_the_engines_own(command_schema: dict) -> None:
    from shepherd_workflow.state import RefusalClass

    published = set(command_schema["properties"]["refusal"]["properties"]["detail"]["enum"])
    assert published == {refusal.value for refusal in RefusalClass}


def test_the_published_statuses_are_exactly_the_ones_the_service_maps(
    stage_run_schema: dict,
) -> None:
    """`status` on the stage run is a copy of the ledger's own vocabulary."""
    from shepherd_workflow.state import StageStatus

    published = set(stage_run_schema["properties"]["stage_run"]["properties"]["status"]["enum"])
    assert published == {status.value for status in StageStatus}


# --- real responses against the published shapes ----------------------------


def test_a_started_job_conforms_to_the_stage_run_shape(
    client: TestClient, stage_run_schema: dict
) -> None:
    body = _start(client)

    Draft202012Validator(stage_run_schema["properties"]["stage_run"]).validate(body)


def test_an_accepted_command_returns_the_SAME_stage_run_shape(
    client: TestClient, stage_run_schema: dict
) -> None:
    started = _start(client)

    leased = client.post(
        f"/v1/stage-runs/{started['stage_run_id']}/commands",
        json={"kind": "lease", "state_version": 0},
        headers=HEADERS,
    )

    assert leased.status_code == 200, leased.text
    Draft202012Validator(stage_run_schema["properties"]["stage_run"]).validate(leased.json())


def test_a_read_back_stage_run_conforms_and_equals_what_the_command_returned(
    client: TestClient, stage_run_schema: dict
) -> None:
    started = _start(client)
    url = f"/v1/stage-runs/{started['stage_run_id']}/commands"
    leased = client.post(url, json={"kind": "lease", "state_version": 0}, headers=HEADERS).json()

    read = client.get(f"/v1/stage-runs/{started['stage_run_id']}", headers=HEADERS)

    assert read.status_code == 200
    Draft202012Validator(stage_run_schema["properties"]["stage_run"]).validate(read.json())
    assert read.json() == leased


def test_a_refused_command_is_a_409_carrying_the_refusal_class_verbatim(
    client: TestClient, stage_run_schema: dict, command_schema: dict
) -> None:
    """The semantic the contract exists to publish: a refusal is an ANSWER."""
    started = _start(client)
    url = f"/v1/stage-runs/{started['stage_run_id']}/commands"
    client.post(url, json={"kind": "lease", "state_version": 0}, headers=HEADERS)

    conflict = client.post(url, json={"kind": "lease", "state_version": 0}, headers=HEADERS)

    assert conflict.status_code == 409
    Draft202012Validator(stage_run_schema["properties"]["error"]).validate(conflict.json())
    Draft202012Validator(command_schema["properties"]["refusal"]).validate(
        {"status": conflict.status_code, "detail": conflict.json()["detail"]}
    )


def test_another_orgs_stage_run_conforms_to_the_404_error_shape(
    client: TestClient, stage_run_schema: dict, command_schema: dict
) -> None:
    started = _start(client)

    response = client.get(f"/v1/stage-runs/{started['stage_run_id']}", headers=OTHER_ORG)

    assert response.status_code == 404
    Draft202012Validator(stage_run_schema["properties"]["error"]).validate(response.json())
    assert 404 in command_schema["properties"]["refusal"]["properties"]["status"]["enum"]


def test_an_unauthenticated_caller_conforms_to_the_error_shape(
    client: TestClient, stage_run_schema: dict
) -> None:
    response = client.get("/v1/stage-runs/srun_whatever")

    assert response.status_code == 401
    Draft202012Validator(stage_run_schema["properties"]["error"]).validate(response.json())


def test_an_unknown_command_kind_is_a_422_with_a_STRING_detail(
    client: TestClient, stage_run_schema: dict
) -> None:
    started = _start(client)

    response = client.post(
        f"/v1/stage-runs/{started['stage_run_id']}/commands",
        json={"kind": "teleport", "state_version": 0},
        headers=HEADERS,
    )

    assert response.status_code == 422
    Draft202012Validator(stage_run_schema["properties"]["error"]).validate(response.json())


def test_a_malformed_body_is_a_422_with_a_LIST_detail(
    client: TestClient, stage_run_schema: dict
) -> None:
    """Both 422 shapes are real, and this is the arm that says so.

    A refused command carries `error` (a string `detail`); a malformed body
    carries `validation_error` (a list). A caller that parses a 422 `detail` as a
    string breaks on the first mistyped field, which is why they are published as
    two shapes rather than one.
    """
    started = _start(client)

    response = client.post(
        f"/v1/stage-runs/{started['stage_run_id']}/commands",
        json={"kind": "lease", "state_versoin": 3, "state_version": 0},
        headers=HEADERS,
    )

    assert response.status_code == 422
    Draft202012Validator(stage_run_schema["properties"]["validation_error"]).validate(
        response.json()
    )
    with pytest.raises(ValidationError):
        Draft202012Validator(stage_run_schema["properties"]["error"]).validate(response.json())


# --- the controls -----------------------------------------------------------


def test_the_validator_rejects_a_stage_run_missing_a_required_field(
    stage_run_schema: dict,
) -> None:
    # Without this, a validator that accepted anything would make every arm
    # above pass while proving nothing about the contract.
    validator = Draft202012Validator(stage_run_schema["properties"]["stage_run"])

    with pytest.raises(ValidationError):
        validator.validate(
            {
                "stage_run_id": "srun_1",
                "job_id": "job_1",
                "workflow": "shepherd.agent_run.v1",
                "stage": "plan",
                "status": "pending",
                "state_version": 0,
                "fence": None,
                # `attempts` deliberately absent
            }
        )


def test_the_command_request_half_rejects_an_unknown_field(command_schema: dict) -> None:
    # `additionalProperties: false` is load-bearing on the PUBLISHED request
    # shape for the reason the module docstring gives: a misspelled
    # `state_version` that is silently dropped applies the command at a version
    # the caller never read.
    validator = Draft202012Validator(command_schema["properties"]["request"])

    validator.validate({"kind": "lease", "state_version": 0})
    with pytest.raises(ValidationError):
        validator.validate({"kind": "lease", "state_version": 0, "state_versoin": 3})
