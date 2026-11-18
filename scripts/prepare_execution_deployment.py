"""Validate deployment inputs and derive subscriptions from the image's exact catalog."""
import argparse
import hashlib
import json
import re
from pathlib import Path

from shepherd_workflow_service.models.definition import Catalog
from shepherd_workflow_service.subscriptions_main import compile_subscriptions


def prepare(values, catalog_bytes, image_uri):
    required = (
        "aws_region", "vpc_id", "certificate_arn", "workflow_catalog_path",
        "workflow_event_bus_name", "workflow_event_bus_arn", "workflow_event_tenants",
        "db_host", "db_name", "db_user", "db_password_secret_arn", "auth_jwks_uri", "auth_audience",
    )
    missing = [key for key in required if not values.get(key)]
    if missing:
        raise ValueError("Missing deployment configuration: " + ", ".join(missing))
    if values.get("environment") != "staging":
        raise ValueError("This rollout entrypoint requires environment=staging")
    pattern = r"[0-9]{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com/[^\s@]+@sha256:[0-9a-f]{64}"
    if not re.fullmatch(pattern, image_uri):
        raise ValueError("Use an immutable private ECR image digest")
    if not values["auth_jwks_uri"].startswith("https://"):
        raise ValueError("Staging JWKS must use HTTPS")
    if not isinstance(values["workflow_event_tenants"], list):
        raise ValueError("workflow_event_tenants must be a JSON array")
    catalog = Catalog.model_validate_json(catalog_bytes)
    if not catalog.workflows:
        raise ValueError("The image catalog must contain workflows")
    result = dict(values, execution_enabled=True, image_uri=image_uri)
    result.update(compile_subscriptions(catalog, values["workflow_event_bus_name"]))
    return result, hashlib.sha256(catalog_bytes).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    values, digest = prepare(
        json.loads(args.config.read_text()), args.catalog.read_bytes(), args.image
    )
    args.output.write_text(json.dumps(values, indent=2) + "\n")
    print("Validated image catalog SHA-256: " + digest)


if __name__ == "__main__":
    main()
