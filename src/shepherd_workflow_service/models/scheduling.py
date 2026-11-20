"""Policy and persisted values for stage-attempt scheduling."""

from dataclasses import dataclass
from datetime import datetime
from math import ceil, isfinite

MAX_STAGE_RUNTIME = 86_400
_MINIMUM_COST_SAMPLES = 10
_COST_ALPHA = 0.2


@dataclass(frozen=True, slots=True)
class TenantPolicy:
    weight: int = 1
    backlog_cap: int = 1000
    in_flight_cap: int = 10
    version: int = 1
    actor: str = "system"

    def __post_init__(self) -> None:
        if type(self.weight) is not int or not 1 <= self.weight <= 10:
            raise ValueError("weight must be an integer between 1 and 10")
        if any(
            type(value) is not int or value < 1
            for value in (self.backlog_cap, self.in_flight_cap, self.version)
        ):
            raise ValueError("caps and policy version must be positive integers")
        if not isinstance(self.actor, str) or not self.actor.strip():
            raise ValueError("policy actor is required")


@dataclass(frozen=True, slots=True)
class EligibleHead:
    tenant_id: str
    stage_run_id: str
    ready_at: datetime
    cost: int

    def __post_init__(self) -> None:
        if type(self.cost) is not int or not 1 <= self.cost <= MAX_STAGE_RUNTIME:
            raise ValueError("cost must be an integer within the stage runtime bounds")


@dataclass(frozen=True, slots=True)
class TenantSchedulingState:
    deficit: int = 0
    active: int = 0


@dataclass(frozen=True, slots=True)
class PoolSchedulingState:
    cursor: str | None = None
    visit_open: bool = False
    admissions: int = 0


@dataclass(frozen=True, slots=True)
class AdmissionDecision:
    head: EligibleHead | None
    pool_state: PoolSchedulingState
    tenant_states: dict[str, TenantSchedulingState]
    age_override: bool = False


@dataclass(frozen=True, slots=True)
class CostEstimate:
    """Running mean during warm-up followed by an exponentially weighted mean."""

    samples: int = 0
    ewma_seconds: float | None = None
    timeouts: int = 0

    def observe(self, seconds: float, timed_out: bool = False) -> "CostEstimate":
        if not isfinite(seconds) or seconds < 0:
            raise ValueError("observed occupancy must be finite and nonnegative")
        seconds = min(seconds, MAX_STAGE_RUNTIME)
        samples = self.samples + 1
        mean = self.ewma_seconds
        if mean is None:
            mean = seconds
        else:
            alpha = 1 / samples if samples <= _MINIMUM_COST_SAMPLES else _COST_ALPHA
            mean += alpha * (seconds - mean)
        return CostEstimate(samples, mean, self.timeouts + int(timed_out))

    def charge(self, fallback: int = 1, maximum: int = MAX_STAGE_RUNTIME) -> int:
        if type(maximum) is not int or not 1 <= maximum <= MAX_STAGE_RUNTIME:
            raise ValueError("maximum charge must be within the stage runtime bounds")
        if type(fallback) is not int or fallback < 1:
            raise ValueError("fallback charge must be a positive integer")
        if self.samples < _MINIMUM_COST_SAMPLES or self.ewma_seconds is None:
            return min(maximum, fallback)
        return max(1, min(maximum, ceil(self.ewma_seconds)))
