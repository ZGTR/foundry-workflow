"""Proves `docker/entrypoint.sh`'s runtime-detection logic without a real
Docker build: `ENTRYPOINT_PYTHON` swaps in a stub script that just echoes
its argv and the `RUNTIME_MODE` it inherited, so this exercises the exact
shell logic the container runs, under both conditions.
"""

from __future__ import annotations

import stat
import subprocess
from pathlib import Path

import pytest

ENTRYPOINT = Path(__file__).parent.parent.parent / "docker/entrypoint.sh"

STUB_PYTHON = """#!/usr/bin/env sh
echo "STUB_PYTHON_ARGS=$*"
echo "STUB_RUNTIME_MODE=${RUNTIME_MODE:-<unset>}"
"""


def _run_entrypoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    env_overrides: dict[str, str],
    args: list[str] | None = None,
) -> subprocess.CompletedProcess:
    stub = tmp_path / "python"
    stub.write_text(STUB_PYTHON)
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)

    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
    monkeypatch.delenv("RUNTIME_MODE", raising=False)
    monkeypatch.setenv("ENTRYPOINT_PYTHON", str(stub))
    for name, value in env_overrides.items():
        monkeypatch.setenv(name, value)

    # env=None (the default): the subprocess inherits this now-modified
    # process environment, so nothing in this file names `os.environ`.
    return subprocess.run(
        ["/bin/sh", str(ENTRYPOINT), *(args or [])],
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_entrypoint_is_executable() -> None:
    mode = ENTRYPOINT.stat().st_mode
    assert mode & stat.S_IXUSR


def test_under_lambda_it_exports_runtime_mode_lambda(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _run_entrypoint(
        tmp_path, monkeypatch, env_overrides={"AWS_LAMBDA_FUNCTION_NAME": "workflow-prod"}
    )

    assert result.returncode == 0
    assert "AWS_LAMBDA_FUNCTION_NAME=workflow-prod" in result.stderr
    assert "STUB_RUNTIME_MODE=lambda" in result.stdout
    assert "STUB_PYTHON_ARGS=-m shepherd_workflow_service.main" in result.stdout


def test_under_ecs_it_exports_runtime_mode_http(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _run_entrypoint(tmp_path, monkeypatch, env_overrides={})

    assert result.returncode == 0
    assert "no Lambda runtime detected" in result.stderr
    assert "STUB_RUNTIME_MODE=http" in result.stdout
    assert "STUB_PYTHON_ARGS=-m shepherd_workflow_service.main" in result.stdout


def test_an_explicit_runtime_mode_is_respected_outside_lambda(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = _run_entrypoint(tmp_path, monkeypatch, env_overrides={"RUNTIME_MODE": "http"})

    assert result.returncode == 0
    assert "STUB_RUNTIME_MODE=http" in result.stdout


def test_the_launch_command_is_identical_in_both_modes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The load-bearing claim of section 6: no code change between targets."""
    lambda_result = _run_entrypoint(
        tmp_path, monkeypatch, env_overrides={"AWS_LAMBDA_FUNCTION_NAME": "f"}
    )
    ecs_result = _run_entrypoint(tmp_path, monkeypatch, env_overrides={})

    def argv_line(result: subprocess.CompletedProcess) -> list[str]:
        return [line for line in result.stdout.splitlines() if line.startswith("STUB_PYTHON_ARGS")]

    assert argv_line(lambda_result) == argv_line(ecs_result)


# ---------------------------------------------------------------------------
# The CMD this ENTRYPOINT is handed
# ---------------------------------------------------------------------------
#
# A container image's ENTRYPOINT receives the CMD as arguments. `image.command`
# in `iac/serverless.yml` and `command:` in an ECS task definition BOTH set the
# CMD, so whether either one can do anything at all is decided here and nowhere
# else. Until 2026-09-05 this script never named "$@", which is why a Lambda
# named `drainOutbox` shipping
#     [/opt/venv/bin/python, -c, "from ...outbox_drain import handle_scheduled_drain"]
# ran the HTTP API server 1440 times a day instead of the import it was recorded
# as performing. Both directions are pinned below, because a forwarding branch
# that also fired on the empty argument list would silently change what every
# ordinary container start runs.


def test_an_explicit_command_is_run_INSTEAD_of_the_http_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without this, `image.command` and ECS `command:` are silently discarded."""
    result = _run_entrypoint(
        tmp_path,
        monkeypatch,
        env_overrides={},
        args=[str(tmp_path / "python"), "-m", "shepherd_workflow_service.ledger_drain_main"],
    )

    assert result.returncode == 0
    assert "STUB_PYTHON_ARGS=-m shepherd_workflow_service.ledger_drain_main" in result.stdout
    assert "STUB_PYTHON_ARGS=-m shepherd_workflow_service.main" not in result.stdout


def test_an_explicit_command_still_inherits_the_runtime_detection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A scheduled one-shot runs under the same RUNTIME_MODE the service would."""
    result = _run_entrypoint(
        tmp_path,
        monkeypatch,
        env_overrides={"AWS_LAMBDA_FUNCTION_NAME": "workflow-prod"},
        args=[str(tmp_path / "python"), "-m", "shepherd_workflow_service.ledger_drain_main"],
    )

    assert result.returncode == 0
    assert "STUB_RUNTIME_MODE=lambda" in result.stdout
    # Asserted here too, deliberately: RUNTIME_MODE is exported before the
    # forwarding branch, so without this line the whole test stays green when
    # forwarding is removed -- measured, it was the one arm M1 did not redden.
    assert "STUB_PYTHON_ARGS=-m shepherd_workflow_service.ledger_drain_main" in result.stdout


def test_with_NO_arguments_the_default_launch_path_is_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The negative control for the two arms above. An ordinary container start
    passes no CMD, and must still reach `main` -- a forwarding branch that fired
    on an empty argument list would exec nothing and exit 0, which reads as a
    healthy start."""
    result = _run_entrypoint(tmp_path, monkeypatch, env_overrides={}, args=[])

    assert result.returncode == 0
    assert "STUB_PYTHON_ARGS=-m shepherd_workflow_service.main" in result.stdout
    assert "explicit command given" not in result.stderr
