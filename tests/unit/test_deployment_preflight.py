import json
from pathlib import Path

import pytest

from scripts.prepare_execution_deployment import prepare

CATALOG = Path("contracts/examples/catalog.v2.json").read_bytes()
IMAGE = "000000000000.dkr.ecr.us-east-1.amazonaws.com/workflow@sha256:" + "a" * 64


def config():
    return {
        "environment": "staging", "aws_region": "us-east-1", "vpc_id": "vpc-example",
        "certificate_arn": "arn:example:certificate", "workflow_catalog_path": "/app/catalog.json",
        "workflow_event_bus_name": "bus", "workflow_event_bus_arn": "arn:example:bus",
        "workflow_event_tenants": ["example"], "db_host": "database.example.invalid",
        "db_name": "workflow", "db_user": "workflow",
        "db_password_secret_arn": "arn:example:secret",
        "auth_jwks_uri": "https://auth.example.invalid/jwks", "auth_audience": "workflow",
        "execution_subscriptions": {"obsolete": {}},
    }


def test_subscriptions_come_from_image_catalog_overriding_drift():
    values, digest = prepare(config(), CATALOG, IMAGE)
    assert values["execution_enabled"] is True
    assert values["image_uri"] == IMAGE
    assert "obsolete" not in values["execution_subscriptions"]
    assert len(digest) == 64
    patterns = [json.loads(s["event_pattern"]) for s in values["execution_subscriptions"].values()]
    assert {p["source"][0] for p in patterns} == {"example-api", "example-worker"}


@pytest.mark.parametrize("key,value", [
    ("environment", "prod"), ("db_host", ""), ("workflow_event_tenants", "*"),
    ("auth_jwks_uri", "http://auth.example.invalid/jwks"),
])
def test_preflight_refuses_incomplete_or_unsafe_deployment(key, value):
    with pytest.raises(ValueError):
        prepare({**config(), key: value}, CATALOG, IMAGE)


def test_preflight_refuses_floating_image():
    with pytest.raises(ValueError, match="immutable"):
        prepare(config(), CATALOG, "workflow:latest")
