"""What a workflow *is*, as a value the caller supplies.

This is the correction that makes the package portable. In the host application
this was extracted from, the same machinery works, but the stage list is a
module-level constant (``WORKFLOW_STAGES``), and the functions that navigate it
(``next_stage``, ``stage_definition``, ``artifact_digest``) close over that
constant. A second consumer with different
stages cannot use any of it, however generic the surrounding code is.

So the constant becomes a value and the functions become its methods. The
reducer then navigates whatever workflow it was handed, and the package contains
no stage name at all.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

from shepherd_contracts import canonical_digest


@dataclass(frozen=True, slots=True)
class StageDefinitionV1:
    """One step. ``key`` is the caller's name for it; this package never reads it."""

    key: str
    input_schema: str
    output_schema: str
    max_attempts: int = 1
    retry_delay_seconds: int = 1

    def __post_init__(self) -> None:
        if type(self.max_attempts) is not int or self.max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        if type(self.retry_delay_seconds) is not int or self.retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be a non-negative integer")
        for name in ("key", "input_schema", "output_schema"):
            if not getattr(self, name):
                raise ValueError(f"StageDefinitionV1.{name} must be non-empty")


@dataclass(frozen=True, slots=True)
class WorkflowDefinitionV1:
    """An immutable, ordered list of stages, identified by a digest of itself.

    The digest matters more than it looks. A job records the workflow digest it
    started under, so a job that began before a stage was inserted is never
    resumed against the new shape -- it is refused, loudly, instead of silently
    executing a workflow nobody planned. That is the whole reason the definition
    is a value with an identity rather than a list.
    """

    workflow_key: str
    stages: tuple[StageDefinitionV1, ...]

    def __post_init__(self) -> None:
        if not self.workflow_key:
            raise ValueError("WorkflowDefinitionV1.workflow_key must be non-empty")
        if not self.stages:
            raise ValueError("a workflow must define at least one stage")
        keys = [s.key for s in self.stages]
        if len(set(keys)) != len(keys):
            dupes = sorted({k for k in keys if keys.count(k) > 1})
            raise ValueError(f"duplicate stage keys: {dupes}")

    @property
    def digest(self) -> str:
        return canonical_digest(
            {
                "workflow_key": self.workflow_key,
                "stages": [
                    {
                        k: v
                        for k, v in asdict(s).items()
                        if k not in {"max_attempts", "retry_delay_seconds"}
                    }
                    if s.max_attempts == 1 and s.retry_delay_seconds == 1
                    else asdict(s)
                    for s in self.stages
                ],
            }
        )

    @property
    def first(self) -> StageDefinitionV1:
        return self.stages[0]

    def stage(self, stage_key: str) -> StageDefinitionV1 | None:
        return next((s for s in self.stages if s.key == stage_key), None)

    def next_after(self, stage_key: str) -> StageDefinitionV1 | None:
        """Return the stage after ``stage_key``, or ``None`` at the end.

        ``None`` is returned both for "this was the last stage" and for "this
        stage is not in this workflow", which are different situations. Callers
        that need to tell them apart ask ``stage()`` first; the reducer does.
        """
        keys = [s.key for s in self.stages]
        if stage_key not in keys:
            return None
        index = keys.index(stage_key) + 1
        return self.stages[index] if index < len(self.stages) else None

    def is_terminal_stage(self, stage_key: str) -> bool:
        return bool(self.stages) and self.stages[-1].key == stage_key
