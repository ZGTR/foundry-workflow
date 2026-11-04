"""The wire shapes for the workflow surface. Pydantic at the edge, values inside.

`extra="forbid"` on every request model, for the reason governance's wire schema
carries it: an unrecognised field is SILENCE, not an error, and a client that
misspells `state_version` would otherwise have its command applied at whatever
version the default supplied. A 422 naming the field is the only answer that
tells the client what actually happened.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from shepherd_workflow_service.ports.workflow_engine import StageRunView


class JobStartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(min_length=1, max_length=64)
    #: The digest of the input this job runs over. It is half of the dispatch key,
    #: so two starts of the same job over the same input are the same work and
    #: return the same stage run rather than creating a second one.
    input_digest: str = Field(min_length=1, max_length=64)


class CommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str = Field(min_length=1, max_length=32)
    #: The version the caller READ. Not optional and not defaulted: a command
    #: that does not say which state it acted on cannot be refused for acting on
    #: a stale one, which is the whole concurrency guarantee.
    state_version: int = Field(ge=0)
    fence: int | None = None
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, payload: dict[str, Any]) -> dict[str, Any]:
        for name, limit in (("payload_digest", 64), ("output_ref", 2048), ("reason", 2048)):
            value = payload.get(name)
            if value is not None and (not isinstance(value, str) or not 1 <= len(value) <= limit):
                raise ValueError(f"{name} must be a nonempty string of at most {limit} characters")
        if "retryable" in payload and type(payload["retryable"]) is not bool:
            raise ValueError("retryable must be a boolean")
        if "provider_pin" in payload and not isinstance(payload["provider_pin"], dict):
            raise ValueError("provider_pin must be an object")
        return payload


class StageRunResponse(BaseModel):
    stage_run_id: str
    job_id: str
    workflow: str
    stage: str
    status: str
    state_version: int
    fence: int | None
    attempts: int
    workflow_digest: str = ""
    input_digest: str = ""
    artifact_digest: str | None = None
    output_ref: str | None = None
    next_stage_run_id: str | None = None
    next_attempt_at: datetime | None = None
    lease_expires_at: datetime | None = None

    @classmethod
    def from_view(cls, view: StageRunView) -> StageRunResponse:
        return cls(
            stage_run_id=view.stage_run_id,
            job_id=view.job_id,
            workflow=view.workflow,
            stage=view.stage,
            status=view.status,
            state_version=view.state_version,
            fence=view.fence,
            attempts=view.attempts,
            workflow_digest=view.workflow_digest,
            input_digest=view.input_digest,
            artifact_digest=view.artifact_digest,
            output_ref=view.output_ref,
            next_stage_run_id=view.next_stage_run_id,
            next_attempt_at=view.next_attempt_at,
            lease_expires_at=view.lease_expires_at,
        )
