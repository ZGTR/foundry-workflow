"""Operator commands for shared-ledger cutover, policy changes and scoped replay."""

import json
from argparse import ArgumentParser
from pathlib import Path

from shepherd_workflow_service.adapters.real.tenant_sql import migrate_tenant_ownership
from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container
from shepherd_workflow_service.models.scheduling import TenantPolicy
from shepherd_workflow_service.models.tenancy import reject_duplicate_json_keys, validate_tenant_id


def _read(path):
    return json.loads(
        path.read_text(encoding="utf-8"), object_pairs_hook=reject_duplicate_json_keys
    )


def _migrate(container):
    with container.workflow_engine.open_connection() as connection, connection.begin():
        migrate_tenant_ownership(connection, container.config.workflow_table_prefix)


def _enable(repository, policy_file, mode):
    source = _read(policy_file)
    policies = {
        validate_tenant_id(tenant): TenantPolicy(**spec)
        for tenant, spec in source["tenants"].items()
    }
    stage_costs = {
        (digest, stage): seconds
        for digest, stages in source.get("stage_cost_seconds", {}).items()
        for stage, seconds in stages.items()
    }
    repository.reconcile_and_enable(
        tenant_policies=policies, mode=mode, stage_cost_fallbacks=stage_costs
    )


def main():
    parser = ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate", help="audit stored ownership and apply byte-exact columns")
    enable = commands.add_parser("enable", help="reconcile existing state and activate admission")
    enable.add_argument("policy_file", type=Path)
    enable.add_argument("--mode", choices=("equal", "weighted", "paused"), default="weighted")
    mode = commands.add_parser("mode", help="change admission strategy without relaxing isolation")
    mode.add_argument("value", choices=("equal", "weighted", "paused"))
    policy = commands.add_parser("policy", help="apply a new version of a tenant policy")
    policy.add_argument("tenant")
    policy.add_argument("policy_file", type=Path)
    replay = commands.add_parser("replay", help="reevaluate delivery using the original owner")
    replay.add_argument("tenant")
    replay.add_argument("event_id")
    commands.add_parser("metrics", help="read aggregate scheduler metrics")
    args = parser.parse_args()

    config = ServiceConfig.from_environment()
    if not config.tenant_http_profile:
        parser.error("operator commands require the tenant ECS profile")
    container = build_container(config)
    repository = container.execution_service.repository

    if args.command in {"migrate", "enable"}:
        _migrate(container)
    if args.command == "enable":
        _enable(repository, args.policy_file, args.mode)
    elif args.command == "mode":
        repository.set_mode(args.value)
    elif args.command == "policy":
        policy = TenantPolicy(**_read(args.policy_file))
        repository.update_policy(validate_tenant_id(args.tenant), policy)
    elif args.command == "replay":
        if not container.tenant_outbox_factory().replay(
            owner=validate_tenant_id(args.tenant), event_id=args.event_id
        ):
            return 1
    elif args.command == "metrics":
        print(json.dumps(repository.scheduler_metrics(), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
