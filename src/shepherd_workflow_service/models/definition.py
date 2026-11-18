"""Strict declarative contracts; no transport or persistence dependencies."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from shepherd_contracts import canonical_digest
from shepherd_workflow.workflow import StageDefinitionV1, WorkflowDefinitionV1

Name = Annotated[str, Field(min_length=1, max_length=64)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class EventSelector(Contract):
    event_type: Name
    producer: Name


class Dispatch(Contract):
    event_type: Name


class Completion(Contract):
    success: EventSelector
    failure: EventSelector
    timeout_seconds: int = Field(default=900, ge=1, le=86400)


class Stage(Contract):
    key: Name
    input_schema: Name
    output_schema: Name
    dispatch: Dispatch
    completion: Completion
    max_attempts: int = Field(default=1, ge=1, le=100)
    retry_delay_seconds: int = Field(default=30, ge=0, le=86400)


class Definition(Contract):
    apiVersion: Literal["foundry.workflow/v2"]
    workflow_key: Name
    version: int = Field(ge=1)
    trigger: EventSelector
    stages: tuple[Stage, ...] = Field(min_length=1)
    completion: Literal["success", "review"] = "success"
    deadline_seconds: int = Field(default=3600, ge=1, le=604800)

    @model_validator(mode="after")
    def unique_stages(self):
        if len({s.key for s in self.stages}) != len(self.stages):
            raise ValueError("duplicate stage keys")
        for stage in self.stages:
            if stage.completion.success == stage.completion.failure:
                raise ValueError("success and failure selectors must differ")
        return self

    @property
    def digest(self) -> str:
        return canonical_digest(self.model_dump(mode="json"))

    def ledger_definition(self) -> WorkflowDefinitionV1:
        return WorkflowDefinitionV1(
            workflow_key=self.workflow_key,
            stages=tuple(
                StageDefinitionV1(
                    key=s.key,
                    input_schema=s.input_schema,
                    output_schema=s.output_schema,
                    max_attempts=s.max_attempts,
                    retry_delay_seconds=s.retry_delay_seconds,
                    timeout_seconds=s.completion.timeout_seconds,
                )
                for s in self.stages
            ),
            completion=self.completion,
            contract_digest=self.digest,
        )


class Catalog(Contract):
    workflows: tuple[Definition, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_versions(self):
        keys = [(w.workflow_key, w.version) for w in self.workflows]
        if len(set(keys)) != len(keys):
            raise ValueError("duplicate workflow key/version")
        return self
