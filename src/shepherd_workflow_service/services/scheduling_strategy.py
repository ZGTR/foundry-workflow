"""Pure weighted deficit round robin over ready stage attempts."""

from bisect import bisect_right
from collections.abc import Iterable, Mapping
from dataclasses import replace
from datetime import datetime, timedelta

from shepherd_workflow_service.models.scheduling import (
    MAX_STAGE_RUNTIME,
    AdmissionDecision,
    EligibleHead,
    PoolSchedulingState,
    TenantPolicy,
    TenantSchedulingState,
)


def _fifo_key(head: EligibleHead) -> tuple[datetime, str, str]:
    return head.ready_at, head.stage_run_id, head.tenant_id


class SchedulingStrategy:
    def __init__(
        self,
        *,
        base_quantum: int = 1,
        max_charge: int = MAX_STAGE_RUNTIME,
        age_threshold: timedelta | None = None,
        age_every: int = 10,
        max_age_debt: int = MAX_STAGE_RUNTIME,
    ) -> None:
        if type(base_quantum) is not int or base_quantum < 1:
            raise ValueError("base quantum must be a positive integer")
        if type(max_charge) is not int or not 1 <= max_charge <= MAX_STAGE_RUNTIME:
            raise ValueError("maximum charge must be within the stage runtime bounds")
        if type(age_every) is not int or age_every < 10:
            raise ValueError("age overrides must be at least ten admissions apart")
        if type(max_age_debt) is not int or not 0 <= max_age_debt <= MAX_STAGE_RUNTIME:
            raise ValueError("age debt must be within the stage runtime bounds")
        if age_threshold is not None and age_threshold < timedelta():
            raise ValueError("age threshold must be nonnegative")
        self.base_quantum = base_quantum
        self.max_charge = max_charge
        self.age_threshold = age_threshold
        self.age_every = age_every
        self.max_age_debt = max_age_debt

    def choose(
        self,
        *,
        heads: Iterable[EligibleHead],
        tenant_policies: Mapping[str, TenantPolicy],
        tenant_states: Mapping[str, TenantSchedulingState],
        pool_state: PoolSchedulingState,
        pool_capacity: int,
        now: datetime,
    ) -> AdmissionDecision:
        """Choose one head and return the state to persist with its lease.

        Provide ready heads even for capped tenants so they retain existing
        credit. The caller owns active counts and updates them with each lease.
        """
        tenant_heads: dict[str, EligibleHead] = {}
        for head in heads:
            if head.ready_at > now:
                continue
            if head.cost > self.max_charge:
                raise ValueError("ready attempt exceeds the pool maximum charge")
            previous = tenant_heads.get(head.tenant_id)
            if previous is None or _fifo_key(head) < _fifo_key(previous):
                tenant_heads[head.tenant_id] = head

        # Drop unused positive credit on empty queues while preserving age debt.
        states = {
            tenant: replace(state, deficit=0)
            if state.deficit > 0 and tenant not in tenant_heads
            else state
            for tenant, state in tenant_states.items()
        }
        eligible: dict[str, EligibleHead] = {}
        for tenant, head in tenant_heads.items():
            state = states.setdefault(tenant, TenantSchedulingState())
            if state.active < tenant_policies[tenant].in_flight_cap:
                eligible[tenant] = head

        if sum(state.active for state in states.values()) >= pool_capacity:
            return AdmissionDecision(None, pool_state, states)

        if pool_state.visit_open and pool_state.cursor not in eligible:
            pool_state = replace(pool_state, visit_open=False)
        if not eligible:
            return AdmissionDecision(None, pool_state, states)

        if self.age_threshold is not None and (pool_state.admissions + 1) % self.age_every == 0:
            overdue = (
                head for head in eligible.values() if now - head.ready_at >= self.age_threshold
            )
            oldest = min(overdue, key=_fifo_key, default=None)
            if (
                oldest is not None
                and states[oldest.tenant_id].deficit - oldest.cost >= -self.max_age_debt
            ):
                return self._admit(oldest, pool_state, states, age_override=True)

        cursor = pool_state.cursor
        if pool_state.visit_open and cursor is not None:
            head = eligible[cursor]
            if states[cursor].deficit >= head.cost:
                return self._admit(head, pool_state, states)

        tenants = sorted(eligible)
        start = bisect_right(tenants, cursor) if cursor is not None else 0
        order = tenants[start:] + tenants[:start]
        quanta = {tenant: tenant_policies[tenant].weight * self.base_quantum for tenant in order}

        # Jump all complete rounds in which no head can qualify. The final
        # partial round still observes cursor order and leaves later credit alone.
        rounds_to_fit = min(
            (eligible[tenant].cost - states[tenant].deficit + quanta[tenant] - 1) // quanta[tenant]
            for tenant in order
        )
        skipped = max(0, rounds_to_fit - 1)
        if skipped:
            for tenant in order:
                self._credit(tenant, states, quanta[tenant], skipped)

        for tenant in order:
            self._credit(tenant, states, quanta[tenant], 1)
            head = eligible[tenant]
            if states[tenant].deficit >= head.cost:
                pool_state = replace(pool_state, cursor=tenant, visit_open=True)
                return self._admit(head, pool_state, states)
        raise AssertionError("at least one eligible head can fit after crediting a full round")

    def _credit(
        self,
        tenant: str,
        states: dict[str, TenantSchedulingState],
        quantum: int,
        rounds: int,
    ) -> None:
        state = states[tenant]
        deficit = min(self.max_charge + quantum, state.deficit + quantum * rounds)
        states[tenant] = replace(state, deficit=deficit)

    @staticmethod
    def _admit(
        head: EligibleHead,
        pool_state: PoolSchedulingState,
        states: dict[str, TenantSchedulingState],
        *,
        age_override: bool = False,
    ) -> AdmissionDecision:
        state = states[head.tenant_id]
        states[head.tenant_id] = replace(state, deficit=state.deficit - head.cost)
        pool_state = replace(pool_state, admissions=pool_state.admissions + 1)
        return AdmissionDecision(head, pool_state, states, age_override)
