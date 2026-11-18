"""Workflow event envelope, compatible with the envelope-neutral comms eventing kit."""

from datetime import datetime
from typing import Any, Literal

from pydantic import Field, model_validator

from shepherd_workflow_service.models.definition import Contract, Name


class Correlation(Contract):
    job_id: Name
    stage_run_id: Name
    workflow_digest: Name
    fence: int = Field(ge=1)


class EventEnvelope(Contract):
    specversion: Literal["workflow.event/v1"] = "workflow.event/v1"
    event_id: Name
    type: Name
    producer: Name
    tenant_id: Name
    occurred_at: datetime = Field(strict=False)
    expires_at: datetime | None = Field(default=None, strict=False)
    correlation: Correlation | None = None
    input_ref: str | None = Field(default=None, min_length=1, max_length=2048)
    output_ref: str | None = Field(default=None, min_length=1, max_length=2048)
    payload_digest: Name | None = None
    retryable: bool = False
    data: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def aware_time(self):
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must include timezone")
        return self
