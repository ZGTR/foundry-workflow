from __future__ import annotations

import pytest

from shepherd_workflow_service.config import ServiceConfig


def test_from_environment_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "SERVICE_ENV",
        "PORT",
        "RUNTIME_MODE",
        "AWS_LAMBDA_FUNCTION_NAME",
        "PRINCIPAL_ISSUER",
        "DB_PATH",
        "EVENT_BUS_MODE",
        "SQS_QUEUE_URL",
        "AWS_REGION",
    ):
        monkeypatch.delenv(var, raising=False)

    config = ServiceConfig.from_environment()

    assert config.env == "local"
    assert config.port == 8080
    assert config.runtime_mode == "http"
    assert config.event_bus_mode == "in_process"


def test_from_environment_reads_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SERVICE_ENV", "staging")
    monkeypatch.setenv("PORT", "9090")
    monkeypatch.setenv("PRINCIPAL_ISSUER", "az-workflow")

    config = ServiceConfig.from_environment()

    assert config.env == "staging"
    assert config.port == 9090
    assert config.issuer == "az-workflow"


def test_from_environment_detects_lambda_by_ambient_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "foundry-workflow-prod")
    monkeypatch.delenv("RUNTIME_MODE", raising=False)

    config = ServiceConfig.from_environment()

    assert config.runtime_mode == "lambda"


def test_sqs_mode_requires_queue_url() -> None:
    with pytest.raises(ValueError, match="sqs_queue_url"):
        ServiceConfig(
            env="local",
            port=8080,
            runtime_mode="http",
            issuer="test",
            db_path=":memory:",
            event_bus_mode="sqs",
            sqs_queue_url=None,
            aws_region=None,
        )


def test_no_module_outside_config_names_the_environment() -> None:
    """The shape check lives in tests/shape/test_layering.py; this test is a
    narrower, config-local restatement: config.py is the only module that
    should ever import `os`."""
    import ast
    from pathlib import Path

    config_source = Path(__file__).parent.parent.parent / "src/shepherd_workflow_service/config.py"
    tree = ast.parse(config_source.read_text())
    imports_os = any(
        isinstance(node, ast.Import) and any(alias.name == "os" for alias in node.names)
        for node in ast.walk(tree)
    )
    assert imports_os, "config.py should be the module importing os"
