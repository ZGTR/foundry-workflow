import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import func, select
from test_declarative_execution import EXAMPLE, dispatched, event, receive, result, rows, setup

from shepherd_workflow_service.adapters.real.sql_execution import SqlExecutionRepository
from shepherd_workflow_service.errors import ExecutionBackpressure, StageRunConflict
from shepherd_workflow_service.models.scheduling import TenantPolicy
from shepherd_workflow_service.services.execution import ExecutionService
from shepherd_workflow_service.services.scheduling_strategy import SchedulingStrategy


def scheduled(container, clock, *, size=1):
    previous = container.execution_service.repository
    return SqlExecutionRepository(
        catalog=previous.catalog,
        engine_factory=previous.engine_factory,
        clock=clock,
        prefix=previous.prefix,
        producer=previous.producer,
        scheduler=SchedulingStrategy(),
        pool_capacity=size,
    )


def use_scheduler(container, clock, *, size=1):
    repository = scheduled(container, clock, size=size)
    container.execution_service = ExecutionService(repository)
    return repository


def admission_rows(repository, name):
    with repository._connection() as connection:
        return connection.execute(select(getattr(repository.admission, name))).mappings().all()


def test_atomic_fanout_duplicate_and_pool_capacity(tmp_path):
    catalog = json.loads(EXAMPLE.read_text())
    catalog["workflows"].append({**catalog["workflows"][0], "workflow_key": "extra"})
    container, clock = setup(tmp_path, catalog)
    repository = use_scheduler(container, clock)
    repository.reconcile_and_enable(tenant_policies={"org": TenantPolicy(backlog_cap=1)})
    trigger = event(clock)
    with pytest.raises(ExecutionBackpressure):
        receive(container, trigger)
    assert not rows(container, "jobs")
    with repository._connection() as connection:
        assert connection.scalar(select(func.count()).select_from(repository.inbox)) == 0
    repository.update_policy("org", TenantPolicy(backlog_cap=2, version=2, actor="operator"))
    accepted = receive(container, trigger)
    assert len(accepted) == 2
    assert receive(container, trigger) == accepted
    with pytest.raises(StageRunConflict, match="event_identity_conflict"):
        receive(container, trigger.model_copy(update={"input_ref": "different"}))
    assert not dispatched(container)
    assert repository.tick() == 0
    assert repository.admit() == 1
    assert repository.admit() == 0
    assert len(admission_rows(repository, "backlog")) == 2
    assert len(admission_rows(repository, "attempts")) == 1


def test_review_preserves_backlog_until_terminal_and_result_replay(tmp_path):
    catalog = json.loads(EXAMPLE.read_text())
    workflow = catalog["workflows"][0]
    workflow["stages"].append({**workflow["stages"][0], "key": "deliver"})
    workflow["completion"] = "review"
    container, clock = setup(tmp_path, catalog)
    repository = use_scheduler(container, clock)
    repository.reconcile_and_enable(tenant_policies={"org": TenantPolicy(backlog_cap=1)})
    receive(container, event(clock))
    repository.admit()
    clock.advance(3)
    completed = result(
        clock,
        dispatched(container)[0],
        payload_digest="digest",
        output_ref="previous",
    )
    receive(container, completed)
    receive(container, completed)
    assert len(dispatched(container)) == 1
    assert admission_rows(repository, "estimates")[0]["samples"] == 1
    with pytest.raises(ExecutionBackpressure):
        receive(container, event(clock, id="another"))

    fresh = use_scheduler(container, clock)
    assert fresh.admit() == 1
    second = dispatched(container)[-1]
    assert second["input_ref"] == "previous"
    receive(container, result(clock, second, id="next", payload_digest="digest2"))
    assert rows(container, "jobs")[0]["status"] == "review_wait"
    assert not admission_rows(fresh, "backlog")
    assert receive(container, event(clock, id="another"))


def test_cutover_reconciles_active_lease_and_recovery_does_not_admit(tmp_path):
    container, clock = setup(tmp_path)
    legacy = container.execution_service
    trigger = event(clock)
    accepted = receive(container, trigger)
    first = dispatched(container)[0]
    repository = use_scheduler(container, clock)
    repository.reconcile_and_enable(tenant_policies={"org": TenantPolicy(backlog_cap=2)})
    assert legacy.tick() == 0
    assert receive(container, trigger) == accepted
    receive(container, event(clock, id="second"))
    assert repository.admit() == 0
    clock.advance(901)
    assert repository.tick() == 1
    assert len(dispatched(container)) == 1
    assert repository.admit() == 1
    assert dispatched(container)[-1]["correlation"]["job_id"] != first["correlation"]["job_id"]
    repository.set_mode("paused")
    assert repository.admit() == 0
    with pytest.raises(ExecutionBackpressure):
        receive(container, event(clock, id="third"))
    receive(container, result(clock, dispatched(container)[-1], payload_digest="digest"))
    assert repository.admit() == 0
    repository.set_mode("equal")
    assert repository.admit() == 0
    clock.advance(30)
    assert repository.admit() == 1
    assert dispatched(container)[-1]["correlation"]["fence"] == 2
    assert admission_rows(repository, "estimates")[0]["timeouts"] == 1


def test_concurrent_sqlite_writers_do_not_overrun_pool_or_backlog(tmp_path):
    container, clock = setup(tmp_path)
    repository = use_scheduler(container, clock, size=2)
    repository.reconcile_and_enable(
        tenant_policies={"org": TenantPolicy(backlog_cap=3, in_flight_cap=2)},
    )

    def submit(index):
        try:
            return receive(container, event(clock, id=f"start-{index}"))
        except ExecutionBackpressure:
            return []

    with ThreadPoolExecutor(max_workers=6) as group:
        assert sum(bool(receipt) for receipt in group.map(submit, range(9))) == 3
        workers = [scheduled(container, clock) for _ in range(6)]
        assert sum(group.map(lambda worker: worker.admit(limit=1), workers)) == 2
    assert len(dispatched(container)) == 2
    assert len(admission_rows(repository, "backlog")) == 3
    assert len(admission_rows(repository, "attempts")) == 2
    assert repository.scheduler_metrics()["PoolSaturation"] == 1


def test_lease_crash_rolls_back_ready_row_and_retries(tmp_path, monkeypatch):
    container, clock = setup(tmp_path)
    repository = use_scheduler(container, clock)
    repository.reconcile_and_enable(tenant_policies={"org": TenantPolicy()})
    receive(container, event(clock))

    def fail(*args, **kwargs):
        raise RuntimeError("process failed before admission commit")

    with monkeypatch.context() as patch:
        patch.setattr(repository.admission, "record_lease", fail)
        with pytest.raises(RuntimeError):
            repository.admit()
    assert not dispatched(container)
    assert rows(container, "stage_runs")[0]["status"] == "pending"
    assert not admission_rows(repository, "attempts")
    assert len(admission_rows(repository, "ready")) == 1
    assert use_scheduler(container, clock).admit() == 1
    assert dispatched(container)[0]["correlation"]["fence"] == 1


def test_registry_filters_fanout_and_reauthorizes_pinned_replays(tmp_path):
    from dataclasses import replace

    from shepherd_contracts import PrincipalMinter
    from shepherd_contracts.errors import Unauthorized

    from shepherd_workflow_service.models.tenancy import (
        TenantRegistry,
        TenantWorkload,
        TriggerGrant,
    )

    catalog = json.loads(EXAMPLE.read_text())
    workflow = catalog["workflows"][0]
    catalog["workflows"] += [
        {**workflow, "version": 2},
        {**workflow, "workflow_key": "another-workflow"},
    ]
    container, clock = setup(tmp_path, catalog)
    repository = use_scheduler(container, clock)
    pinned, newer, sibling = repository.catalog.load().workflows
    workload = TenantWorkload(
        issuer="https://issuer.example",
        client_id="submitter",
        subject="subject",
        tenant_id="org",
        producer="example-api",
        scopes=("workflow:submit", "workflow:read"),
        certificate_thumbprints=("A" * 43,),
        active=True,
    )
    first = TriggerGrant(
        tenant_id="org",
        producer="example-api",
        event_type="example.requested",
        workflow_digest=pinned.digest,
    )
    second = first.model_copy(update={"workflow_digest": sibling.digest})
    registry = TenantRegistry(workloads=(workload,), triggers=(first,))
    principal = PrincipalMinter(issuer="test").mint(
        org_id="org",
        subject_id="example-api",
        scopes=["workflow:submit", "workflow:read"],
        attributes={
            "principal_class": "tenant",
            "token_issuer": workload.issuer,
            "client_id": workload.client_id,
            "subject": workload.subject,
        },
    )
    trigger = event(clock)
    empty = registry.model_copy(update={"triggers": ()})
    repository.tenant_registry = empty
    with pytest.raises(ValueError, match="must belong to the tenant registry"):
        repository.reconcile_and_enable(tenant_policies={"foreign": TenantPolicy()})
    repository.reconcile_and_enable(tenant_policies={"org": TenantPolicy()})
    service = ExecutionService(repository, tenant_registry=empty)
    with pytest.raises(Unauthorized):
        service.receive(trigger, principal)
    assert not rows(container, "jobs")

    full = registry.model_copy(update={"triggers": (first, second)})
    repository.tenant_registry = full
    service.tenant_registry = full
    accepted = service.receive(trigger, principal)
    assert len(accepted) == 2
    assert {row["workflow_digest"] for row in rows(container, "jobs")} == {
        pinned.digest,
        sibling.digest,
    }
    assert newer.digest != pinned.digest
    assert service.receive(trigger, principal) == accepted
    assert len(service.stages(accepted[0], principal)) == 1
    with pytest.raises(Unauthorized):
        service.stages(accepted[0], replace(principal, scopes=frozenset({"workflow:submit"})))

    repository.tenant_registry = registry
    service.tenant_registry = registry
    with pytest.raises(Unauthorized):
        service.receive(trigger, principal)
    assert len(rows(container, "jobs")) == 2
    filtered = service.receive(event(clock, id="second-trigger"), principal)
    assert len(filtered) == 1
    assert (
        next(row for row in rows(container, "jobs") if row["job_id"] == filtered[0])[
            "workflow_digest"
        ]
        == pinned.digest
    )


def test_pool_cursor_and_deficit_survive_single_admission_worker_restart(tmp_path):
    container, clock = setup(tmp_path)
    repository = use_scheduler(container, clock)
    repository.reconcile_and_enable(
        tenant_policies={
            "org": TenantPolicy(weight=2),
            "other": TenantPolicy(weight=1),
        }
    )
    for tenant in ("org", "other"):
        for index in range(4):
            receive(
                container,
                event(clock, id=f"{tenant}-{index}").model_copy(
                    update={"tenant_id": tenant},
                ),
            )

    admitted = []
    for index in range(6):
        worker = use_scheduler(container, clock)
        assert worker.admit(limit=1) == 1
        dispatch = dispatched(container)[-1]
        admitted.append(dispatch["tenant_id"])
        completion = result(clock, dispatch, id=f"result-{index}", payload_digest="digest")
        receive(container, completion.model_copy(update={"tenant_id": dispatch["tenant_id"]}))
    assert admitted == ["org", "org", "other", "org", "org", "other"]


def test_expiring_a_queued_job_releases_backlog_without_a_lease(tmp_path):
    catalog = json.loads(EXAMPLE.read_text())
    catalog["workflows"][0]["deadline_seconds"] = 5
    container, clock = setup(tmp_path, catalog)
    repository = use_scheduler(container, clock)
    repository.reconcile_and_enable(tenant_policies={"org": TenantPolicy(backlog_cap=1)})
    receive(container, event(clock))
    clock.advance(5)
    assert repository.admit() == 0
    with pytest.raises(ExecutionBackpressure):
        receive(container, event(clock, id="second"))
    assert repository.tick() == 1
    assert repository.tick() == 0
    assert not admission_rows(repository, "backlog")
    assert not admission_rows(repository, "ready")
    assert not admission_rows(repository, "attempts")
    assert rows(container, "jobs")[0]["status"] == "expired"
    assert receive(container, event(clock, id="second"))


def test_cost_fallback_yields_to_persisted_server_observations(tmp_path):
    container, clock = setup(tmp_path)
    repository = use_scheduler(container, clock)
    definition = repository.catalog.load().workflows[0]
    repository.reconcile_and_enable(
        tenant_policies={"org": TenantPolicy()},
        stage_cost_fallbacks={(definition.digest, definition.stages[0].key): 5},
    )
    for index in range(11):
        if index == 10:
            repository = use_scheduler(container, clock)
        receive(container, event(clock, id=f"sample-{index}"))
        assert repository.admit(limit=1) == 1
        dispatch = dispatched(container)[-1]
        clock.advance(2)
        receive(container, result(clock, dispatch, id=f"done-{index}", payload_digest="sample"))
    estimates = admission_rows(repository, "estimates")
    assert len(estimates) == 1
    assert estimates[0]["samples"] == 11
    assert repository.scheduler_metrics()["ChargedSlotSeconds"] == 52
    assert repository.scheduler_metrics()["ObservedSlotSeconds"] == 22
