# Multi-tenant workflow execution plan

One shared Foundry deployment will accept declarative workflows for independently authenticated clients. Each tenant owns its jobs and data. When tenants compete for capacity, each receives a positive share of eligible dispatches based on numeric entitlement and measured work. Tenant onboarding requires passing the [security release gate](tenant-security.md#release-gate).

## Research and decisions

| Reference | What Foundry adopts |
| --- | --- |
| AWS Batch fair-share scheduling | Set numeric entitlement and account for work consumed. Batch uses weight factors, a usage decay period and compute reservation. Foundry uses direct weights where 2 means twice the share of 1. [AWS Batch](https://docs.aws.amazon.com/batch/latest/userguide/fair-share-scheduling.html) |
| Kubernetes API Priority and Fairness | Separate concurrency limits from queue order, account for occupied capacity, and let eligible tenants use spare capacity. [Kubernetes](https://kubernetes.io/docs/concepts/cluster-administration/flow-control/) |
| Amazon SQS fair queues | Measure queue dwell time and in-flight concentration during bursts. SQS Standard fair queues can improve transport dwell for quiet groups. Foundry enforces weighted dispatch and admission at the workflow boundary. [Amazon SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fair-queues.html) |
| Shreedhar and Varghese, Deficit Round Robin | Carry unspent credit across turns so larger work items can qualify. Foundry adapts this algorithm to non-preemptible workflow attempts. [Original paper](https://repository.library.washu.edu/cse_research/339/) |

Strict priority can indefinitely delay a lower-weight tenant. Counting jobs alone can let long stages dominate capacity. Use weighted deficit round robin over **eligible stage attempts**, including starts, continuations and retries. FIFO within a tenant uses the current attempt's ready time and a stable stage-run ID tie-breaker. Retry backoff and stages waiting for review are ineligible. Start with one scheduling pool.

## Numeric policy

- An operator-managed, versioned tenant policy holds a positive integer weight, backlog cap and in-flight cap. Enrollment sets these from capacity entitlement and service objectives. Default weight is 1 and the proposed range is 1–10. Request data and queue pressure cannot grant a larger weight. Record the policy version and actor for every change.
- Charge estimated slot-time. Use a bounded rolling estimate from server-observed durations keyed by tenant, workflow definition digest and stage. Use an operator-defined per-stage fallback and maximum based on the approved executor runtime, and a cold-start default of 1 unit where no calibrated estimate exists. Lease-to-result is a proxy that includes publication and transport delay. Record timeouts separately and include occupied duration so timeout-heavy workloads are accounted for. Use integer units of one slot-second and initialize an exponentially weighted average with alpha 0.2 after at least 10 samples. Clamp estimates to the approved stage runtime, up to the workflow contract maximum of 86,400 seconds. Advance empty credit rounds mathematically when no eligible head can afford its cost so high-cost stages do not cause a busy loop. Capture the cost at admission and compare with actual occupied time. Validate the units and bounds on staging traces.
- Each visit credits the tenant with its weight times the base quantum. Admit and debit while the head fits and tenant and pool capacity permit. Persist the cursor, deficit and whether a visit is open across transactions so one-item batches cannot recredit a tenant. Empty ready queues lose residual credit. Tenants at their cap skip accrual. Bound deficit to at least the maximum charge with room for the current quantum. Advance rounds when credit is the sole constraint so the pool stays busy. With equal costs and two continually eligible tenants, weights 2 and 1 target a 2:1 dispatch share. With mixed costs the share applies to charged work, with observed slot-time tracked separately. Dispatch fairness is an estimated work guarantee. Staging must report duration-estimation error and realized capacity share. Do not claim a realized capacity allocation until that evidence supports it.
- Once an eligible head crosses a configured dwell threshold, reserve at most one in every ten admissions for the oldest eligible overdue head. Unused reserved turns flow back to weighted dispatch. Persist the cadence and charge the full stage cost, bounding any negative deficit and forgoing the override if the debt limit would be exceeded. Track oldest eligible wait, missed deadlines and override rate. Non-preemptible work, arbitrary overload and expired deadlines put limits on achievable tail latency.
- Enforce a fleet-wide active-attempt cap and per-tenant hard limits. Allow tenants to borrow spare share up to their caps. Admit again as leases finish. The executor checks dispatch expiry, enforces bounded runtime or cancellation and deduplicates external effects. A ledger slot becoming free does not itself terminate a remote process.

Weights express the capacity allocation decision. Measured duration and queue age drive adaptation in the first release. Treat all proposed constants as staging starting points.

## Fit with the existing service

The v2 events path already commits inbox receipts and definition snapshots and currently leases first attempts and continuations immediately. Move every v2 lease to explicit dispatch admission. Keep the v1 manual command API behind internal authorization because direct leases can bypass admission. Tenant-facing progress reads use verified ownership.

1. Authenticate and authorize HTTP or trusted pub/sub intake. Commit the inbox, job binding and pending stage together. HTTP 202 acknowledges durable acceptance. Charge one backlog unit per accepted nonterminal job throughout pending, leased and retry states. Release the backlog unit exactly once on terminal job success, failure, expiry, cancellation or review_wait. The existing ledger treats review_wait as terminal. Completing an intermediate stage retains the backlog unit. Charge existing in-flight attempts at cutover and exclude unrelated internal legacy jobs. Apply the backlog quota to a whole trigger fan-out and return retryable backpressure with a bounded Retry-After for new submissions. Keep result intake and expiry maintenance independent of submission throttles.
2. Persist eligibility, scheduler state and active-slot accounting in the shared ledger. A short per-pool transaction selects indexed tenant heads, checks quotas and deadlines, leases via the existing reducer, and inserts the fenced dispatch outbox event atomically. Use a scheduler row lock on MySQL and BEGIN IMMEDIATE with bounded lock retries on SQLite. Acquire the pool lock before tenant, inbox or stage locks in any participating writer. Carry the exact tenant owner through stage and previous-output lookups. Specify consistent lock ordering for intake, admission, completion and recovery. Preserve event schemas, idempotency, and definition snapshots.
3. Separate maintenance of expired jobs and leases from admission. Keep retries, deadlines, fences and terminal transitions with the reducer. Release slots and sample cost at most once per attempt. Run admission and maintenance even when shared queue intake is disabled. Use independent work loops or bounded I/O budgets so long polls and slow destinations cannot stall other worker duties. Measure query plans and lock contention.
4. Keep MVC controllers limited to parsing and serialization around service calls. Use typed policy and state objects, a pure Scheduling Strategy, transport Adapters, the existing Unit of Work and one Coordinator for the admission transaction. Constructor dependency injection is the IoC mechanism in container.py. Give each class one responsibility and centralize tenant identity and ownership rules. Preserve out-of-band (OOB) dispatch via the transactional outbox and pub/sub. Broker and callback calls occur outside admission, lease and delivery claim transactions. Preserve trusted ownership and lease fencing in per-destination publication state. Follow provenance checks if changes to the vendored reducer are required.
5. Apply an expand migration and reconcile pending stages, due retries, outstanding leases and unpublished outbox rows before enabling weighted admission. Coordinate the writer cutover so immediate and scheduled writers cannot both lease work. Roll back the policy through the same admission path using equal weights. If the path must be paused, stop tenant submissions while leases and outbox messages drain. Keep tenant authorization enforced throughout. Fence enablement with shared database policy and quiesce older writers; do not rely on mixed ECS environment flags to coordinate cutover.

## Acceptance and rollout

| Gate | Evidence |
| --- | --- |
| Tenant isolation | Two tenants with independent identities pass the [negative and positive isolation matrix](tenant-security.md#release-gate) on production-like ingress, data and egress. |
| Weighted share | After warm-up, deterministic saturated runs over at least 1,000 admissions come within 5 percentage points of target charged share, using weights 2 and 1 without age overrides. Repeat with unequal costs. Record actual occupied-slot share and explain divergence. |
| Quiet-tenant protection | Under noisy traffic and mixed durations, compare quiet-tenant p95/p99 ready-to-lease dwell time and maximum eligible wait against baseline. Set staging targets from real capacity and timeout bounds. Verify continuously eligible tenants progress, age overrides respect their budget, and a lone tenant can use spare capacity up to its cap. |
| Durability and quotas | Race MySQL workers, restart during a scheduling visit, inject database and broker failures, and replay results. Verify no duplicate lease or accounting, early retry, quota overrun or cross-tenant change. Run functional scenarios in SQLite. Verify a multi-stage review workflow retains quota through intermediate success and releases it when the final stage reaches review_wait. |
| Operations | Collect submission and ready-to-lease dwell, lease-to-result, head age, charged versus observed share, saturation, backlog, in-flight, retry, timeout, age overrides and auth rejection. Keep CloudWatch dimensions bounded and tenant drilldowns access-controlled. Demonstrate migration and rollback before adding tenants. |

Stage with test tenants, compare shadow decisions against production-shaped traces, then pilot with a small allowlisted cohort. Record observed capacity, dwell targets, cost estimate error and rollback results before broad rollout.

The reproducible scheduler regression uses one slot, a burst of 300 mixed-cost
attempts from the noisy tenant, and 40 periodic attempts from the quiet tenant.
With weights of 2 and 1, quiet-tenant p95/p99 ready-to-lease dwell was 84/88
simulated seconds, compared with 2,195/2,199 under FIFO. Use production-shaped
arrivals and observed executor durations for staging acceptance. [Scheduler test](../tests/unit/test_scheduling_strategy.py)

## Implementation sequence

Track the rollout in [GitHub epic #3](https://github.com/ZGTR/foundry-workflow/issues/3).

1. Tenant identity and HTTPS ingress, including route and operation scopes. [#4](https://github.com/ZGTR/foundry-workflow/issues/4)
2. Tenant-authorized pub/sub, artifact access and outbound delivery. [#6](https://github.com/ZGTR/foundry-workflow/issues/6)
3. Durable dispatch admission, quotas and migration. [#5](https://github.com/ZGTR/foundry-workflow/issues/5)
4. Weighted strategy, measured job cost and dwell protection. [#7](https://github.com/ZGTR/foundry-workflow/issues/7)
5. Isolation tests across transports, load verification and pilot rollout. [#8](https://github.com/ZGTR/foundry-workflow/issues/8)

Chibo should start with the security work and follow the dependency order. Each item should be independently reviewable and document its acceptance evidence. For the new tenant profile, enable only the ingress and egress paths that have passed verification.
