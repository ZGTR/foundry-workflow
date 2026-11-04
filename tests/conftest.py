"""Shared fixtures. One thing only: where the workflow definition lives.

`build_container()` refuses to start without a workflow definition, on purpose --
a workflow service with no workflow accepts jobs it can never advance. Every
test that builds a container therefore needs a real definition file, and pointing
them all at the repository's own `contracts/workflow.v1.json` means the file the
service ships is the file the tests exercise. A fixture that wrote its own
two-stage workflow would be the one shape nothing deployed ever runs.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_DEFINITION_PATH = str(REPO_ROOT / "contracts" / "workflow.v1.json")
