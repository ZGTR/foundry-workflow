from collections import deque
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from math import ceil

import pytest

from shepherd_workflow_service.models.scheduling import (
    MAX_STAGE_RUNTIME,
    CostEstimate,
    EligibleHead,
    PoolSchedulingState,
    TenantPolicy,
    TenantSchedulingState,
)
from shepherd_workflow_service.services.scheduling_strategy import SchedulingStrategy

NOW = datetime(2026, 11, 20, tzinfo=UTC)


def _head(tenant: str, cost: int = 1, stage: str = "stage", age: int = 0) -> EligibleHead:
    return EligibleHead(tenant, stage, NOW - timedelta(seconds=age), cost)


@pytest.mark.parametrize("costs", [(1, 1), (3, 17)])
def test_saturated_weights_allocate_charged_share(costs):
    strategy = SchedulingStrategy()
    policies = {"a": TenantPolicy(weight=2), "b": TenantPolicy(weight=1)}
    heads = [_head("a", costs[0]), _head("b", costs[1])]
    state = PoolSchedulingState()
    tenants = {}
    charged = {"a": 0, "b": 0}
    for index in range(1_200):
        decision = strategy.choose(
            heads=heads,
            tenant_policies=policies,
            tenant_states=tenants,
            pool_state=state,
            pool_capacity=1,
            now=NOW,
        )
        assert decision.head is not None
        assert not decision.age_override
        if index >= 200:
            charged[decision.head.tenant_id] += decision.head.cost
        state, tenants = decision.pool_state, decision.tenant_states
    assert charged["a"] / sum(charged.values()) == pytest.approx(2 / 3, abs=0.05)


def test_quiet_tenant_progresses_and_noisy_tenant_uses_spare_slots():
    strategy = SchedulingStrategy()
    policies = {"noisy": TenantPolicy(weight=10, in_flight_cap=2), "quiet": TenantPolicy()}
    noisy = _head("noisy")
    quiet = _head("quiet", cost=3, age=1)
    state = PoolSchedulingState()
    tenants = {}
    served = []
    for index in range(100):
        decision = strategy.choose(
            heads=[noisy, quiet] if index >= 30 else [noisy],
            tenant_policies=policies,
            tenant_states=tenants,
            pool_state=state,
            pool_capacity=3,
            now=NOW,
        )
        assert decision.head is not None
        served.append(decision.head.tenant_id)
        state, tenants = decision.pool_state, decision.tenant_states
    assert served[:30] == ["noisy"] * 30
    assert served.index("quiet") < 60

    states = {"noisy": TenantSchedulingState(deficit=9, active=2)}
    decision = strategy.choose(
        heads=[noisy, quiet],
        tenant_policies=policies,
        tenant_states=states,
        pool_state=state,
        pool_capacity=3,
        now=NOW,
    )
    assert decision.head == quiet
    assert decision.tenant_states["noisy"] == states["noisy"]


def _quiet_tenant_waits(strategy: SchedulingStrategy | None) -> list[float]:
    queues = {
        "noisy": deque(_head("noisy", cost=(2, 7, 13)[i % 3], stage=str(i)) for i in range(300)),
        "quiet": deque(
            EligibleHead("quiet", str(i), NOW + timedelta(seconds=1 + 4 * i), 2)
            for i in range(40)
        ),
    }
    policies = {"noisy": TenantPolicy(weight=2), "quiet": TenantPolicy(weight=1)}
    now, pool, tenants = NOW, PoolSchedulingState(), {}
    waits = []
    while any(queues.values()):
        heads = [queue[0] for queue in queues.values() if queue]
        now = max(now, min(head.ready_at for head in heads))
        if strategy is None:
            selected = min(heads, key=lambda head: head.ready_at)
        else:
            decision = strategy.choose(
                heads=heads, tenant_policies=policies, tenant_states=tenants,
                pool_state=pool, pool_capacity=1, now=now,
            )
            selected = decision.head
            assert selected is not None
            pool, tenants = decision.pool_state, decision.tenant_states
        queues[selected.tenant_id].popleft()
        if selected.tenant_id == "quiet":
            waits.append((now - selected.ready_at).total_seconds())
        now += timedelta(seconds=selected.cost)
    return waits


def test_weighted_scheduling_reduces_quiet_tenant_tail_dwell_against_fifo():
    weighted = _quiet_tenant_waits(SchedulingStrategy(age_threshold=timedelta(seconds=60)))
    fifo = _quiet_tenant_waits(None)
    assert len(weighted) == len(fifo) == 40
    for quantile in (0.95, 0.99, 1):
        index = ceil(quantile * len(fifo)) - 1
        assert sorted(weighted)[index] < sorted(fifo)[index] / 4


def test_restart_during_a_visit_preserves_credit_through_full_pool():
    policies = {"a": TenantPolicy(weight=2, in_flight_cap=1), "b": TenantPolicy()}
    heads = [_head("a"), _head("b")]
    kwargs = {"heads": heads, "tenant_policies": policies, "pool_capacity": 1, "now": NOW}
    first = SchedulingStrategy().choose(
        tenant_states={}, pool_state=PoolSchedulingState(), **kwargs
    )
    assert first.head == heads[0]
    persisted = PoolSchedulingState(**asdict(first.pool_state))
    assert persisted.visit_open
    tenants = {"a": TenantSchedulingState(**asdict(first.tenant_states["a"]))}
    full = SchedulingStrategy().choose(
        tenant_states={"a": replace(tenants["a"], active=1)}, pool_state=persisted, **kwargs
    )
    assert full.head is None
    assert full.pool_state == persisted
    second = SchedulingStrategy().choose(tenant_states=tenants, pool_state=persisted, **kwargs)
    assert second.head == heads[0]
    assert second.tenant_states["a"].deficit == 0
    third = SchedulingStrategy().choose(
        tenant_states=second.tenant_states, pool_state=second.pool_state, **kwargs
    )
    assert third.head == heads[1]


def test_fifo_tie_break_and_credit_round_jump():
    later = _head("a", cost=MAX_STAGE_RUNTIME, stage="0")
    first = _head("a", cost=MAX_STAGE_RUNTIME, stage="1", age=1)
    second = _head("a", cost=MAX_STAGE_RUNTIME, stage="2", age=1)
    decision = SchedulingStrategy().choose(
        heads=[second, later, first],
        tenant_policies={"a": TenantPolicy(weight=10)},
        tenant_states={"a": TenantSchedulingState(deficit=-15), "empty": TenantSchedulingState(8)},
        pool_state=PoolSchedulingState(cursor="gone", visit_open=True),
        pool_capacity=10,
        now=NOW,
    )
    assert decision.head == first
    assert decision.tenant_states["a"].deficit == 5
    assert decision.tenant_states["empty"].deficit == 0


def test_age_override_uses_one_per_ten_and_charges_full_cost():
    strategy = SchedulingStrategy(age_threshold=timedelta(seconds=30), max_age_debt=12)
    youngest = _head("a", age=12)
    oldest = _head("b", cost=10, age=60)
    policies = {"a": TenantPolicy(weight=10), "b": TenantPolicy()}
    state = PoolSchedulingState()
    tenants = {}
    overrides = []
    for index in range(35):
        decision = strategy.choose(
            heads=[youngest, oldest],
            tenant_policies=policies,
            tenant_states=tenants,
            pool_state=state,
            pool_capacity=2,
            now=NOW,
        )
        if decision.age_override:
            assert decision.head == oldest
            assert decision.tenant_states["b"].deficit == (
                tenants.get("b", TenantSchedulingState()).deficit - oldest.cost
            )
            overrides.append(index + 1)
        state, tenants = decision.pool_state, decision.tenant_states
    assert overrides == [10]
    assert tenants["b"].deficit >= -12


def test_unspent_age_turn_flows_to_weighted_dispatch_and_does_not_carry():
    strategy = SchedulingStrategy(age_threshold=timedelta(seconds=30))
    policies = {"a": TenantPolicy(weight=2), "b": TenantPolicy()}
    state = PoolSchedulingState(cursor="a", visit_open=True, admissions=9)
    decision = strategy.choose(
        heads=[_head("a"), _head("b")],
        tenant_policies=policies,
        tenant_states={"a": TenantSchedulingState(2)},
        pool_state=state,
        pool_capacity=1,
        now=NOW,
    )
    assert decision.head == _head("a")
    assert not decision.age_override
    followup = strategy.choose(
        heads=[_head("a"), _head("b", age=30)],
        tenant_policies=policies,
        tenant_states=decision.tenant_states,
        pool_state=decision.pool_state,
        pool_capacity=1,
        now=NOW,
    )
    assert followup.head == _head("a")
    assert not followup.age_override


def test_cost_estimate_warms_then_accounts_for_timed_out_occupancy():
    estimate = CostEstimate()
    assert estimate.charge() == 1
    for _ in range(9):
        estimate = estimate.observe(10)
    assert estimate.charge(fallback=3) == 3
    estimate = estimate.observe(20, timed_out=True)
    assert estimate.charge() == 11
    estimate = estimate.observe(21, timed_out=True)
    assert estimate.charge() == 13
    assert estimate.timeouts == 2
    assert estimate.charge(maximum=12) == 12
    assert CostEstimate().charge(fallback=123, maximum=20) == 20
    assert CostEstimate().observe(2 * MAX_STAGE_RUNTIME).ewma_seconds == MAX_STAGE_RUNTIME
    with pytest.raises(ValueError):
        estimate.observe(float("inf"))


@pytest.mark.parametrize(
    "invalid",
    [{"weight": 11}, {"backlog_cap": 0}, {"in_flight_cap": 0}, {"version": 0}, {"actor": " "}],
)
def test_policy_rejects_invalid_entitlement(invalid):
    with pytest.raises(ValueError):
        TenantPolicy(**invalid)
