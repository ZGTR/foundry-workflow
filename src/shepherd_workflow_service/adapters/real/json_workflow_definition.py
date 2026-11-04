"""Loads the workflow this deployment runs, from a JSON file it is pointed at.

The workflow is DATA, not code, and that is the package's central correction:
`WorkflowDefinitionV1` exists because the system this was extracted from held its
stage list in a module-level constant, which no second consumer could reuse. A
service that hardcoded its stages back into Python would undo that in one step.

Failures here are `ConfigurationError`, never a silent default. A service that
starts with an empty or wrong workflow accepts jobs it can never advance, and the
symptom — every command refused as `workflow_digest_mismatch` — points at the
caller rather than at the file that is actually wrong.
"""

from __future__ import annotations

import json
from pathlib import Path

from shepherd_contracts.errors import ConfigurationError
from shepherd_workflow.workflow import StageDefinitionV1, WorkflowDefinitionV1


def load_workflow_definition(path: str) -> WorkflowDefinitionV1:
    """Read ``path`` and return the workflow it declares."""
    file = Path(path)
    try:
        raw = json.loads(file.read_text())
    except FileNotFoundError as exc:
        raise ConfigurationError(
            f"WORKFLOW_DEFINITION_PATH={path!r} does not exist; this service cannot "
            "run a workflow it has not been given"
        ) from exc
    except json.JSONDecodeError as exc:
        raise ConfigurationError(
            f"WORKFLOW_DEFINITION_PATH={path!r} is not valid JSON: {exc}"
        ) from exc

    if not isinstance(raw, dict) or "workflow_key" not in raw or "stages" not in raw:
        raise ConfigurationError(
            f"{path!r} must be an object with 'workflow_key' and 'stages'; got keys "
            f"{sorted(raw) if isinstance(raw, dict) else type(raw).__name__}"
        )

    try:
        stages = tuple(
            StageDefinitionV1(
                key=stage["key"],
                input_schema=stage["input_schema"],
                output_schema=stage["output_schema"],
                max_attempts=stage.get("max_attempts", 1),
                retry_delay_seconds=stage.get("retry_delay_seconds", 1),
            )
            for stage in raw["stages"]
        )
        return WorkflowDefinitionV1(workflow_key=raw["workflow_key"], stages=stages)
    except (KeyError, TypeError, ValueError) as exc:
        # ValueError covers the definition's own validation -- duplicate stage
        # keys, an empty stage list, an empty schema name. Re-raised as a
        # configuration error because that is what it is: the FILE is wrong.
        raise ConfigurationError(f"{path!r} does not describe a valid workflow: {exc}") from exc
