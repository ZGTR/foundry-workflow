from __future__ import annotations

import ast
from pathlib import Path

SRC = Path(__file__).parents[2] / "src/shepherd_workflow_service"


def _service_locator_reads(root: Path) -> list[Path]:
    violations = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text())
        if any(
            isinstance(node, ast.Attribute)
            and node.attr == "container"
            and isinstance(node.value, ast.Attribute)
            and node.value.attr == "state"
            for node in ast.walk(tree)
        ):
            violations.append(path)
    return violations


def test_delivery_uses_injected_dependencies() -> None:
    assert _service_locator_reads(SRC) == []


def test_service_locator_detector_proves_both_directions(tmp_path: Path) -> None:
    clean = tmp_path / "clean.py"
    clean.write_text("def route(service):\n    return service.run()\n")
    assert _service_locator_reads(tmp_path) == []
    offender = tmp_path / "offender.py"
    offender.write_text(
        "def route(request):\n"
        "    return request.app.state.container.service.run()\n"
    )
    assert _service_locator_reads(tmp_path) == [offender]
