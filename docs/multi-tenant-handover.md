# Multi-tenant workflow handover

## Original scope

Run independently authenticated clients on one deployment of the generic
declarative workflow service. Give each tenant a positive numeric share of
dispatch capacity, adapt to measured work, and keep quieter tenants progressing
when another tenant fills the queue. Make tenant isolation the release gate.
Research API keys and machine-to-machine tokens, support authentication in both
directions, and deliver reusable mutual TLS infrastructure and onboarding
guidance.

The request also covered industry research, Markdown plans, scoped GitHub work
items, iterative adversarial review, and implementation using the service's
controller and dependency-injection boundaries. The requested design principles
were SRP, DRY, IoC, constructor dependency injection, MVC, object-oriented
Strategy and Adapter patterns, pub/sub, and out-of-band delivery. Chiba was
requested for implementation cost control. The runner rejected that model ID.
Astra at high reasoning reviewed the plans through three rounds and gave the
implementation a separate green code review. The initial coding sub-agents
used GPT-6.2 Sol.

## What shipped to development

| Area | Delivered |
| --- | --- |
| Research and planning | Compared AWS Batch, SQS fair queues, Kubernetes priority and fairness, and Deficit Round Robin. Recorded the [execution plan](multi-tenant-execution.md), [isolation plan](tenant-security.md), and scoped [GitHub epic and work items](https://github.com/ZGTR/foundry-workflow/issues/3). |
| Tenant API | Opt-in ECS API and worker share one ledger. The application binds a verified certificate and short-lived OAuth token to an active workload, scopes each operation, and checks tenant ownership of jobs and stages. Tenant-facing routes expose only the supported v2 surface. |
| Authentication and ingress | Terraform configures the ALB for mTLS with versioned CA and revocation material. The origin uses private HTTPS and an immediate-peer check for proxy-supplied identity. The reusable [mTLS guide](service-mtls.md) covers onboarding, rotation, revocation, and verification. |
| Outbound isolation | Owner-bound transactional outbox delivery uses private EventBridge and tenant-specific signed HTTPS callbacks. The worker rechecks callback authorization before sending. Artifact references are checked against tenant-scoped prefixes. Storage ACLs and receiver verification belong to each deployment. |
| Fair scheduling | Durable weighted deficit round robin uses positive numeric weights, bounded runtime estimates, tenant and pool caps, and a configurable queue-age safeguard. Fenced admission and delivery persist across restarts. |
| Operations | Operator commands cover ownership migration and audit, scheduling enablement, policy updates, safe policy rollback, metrics, and owner-scoped replay. [Operations runbook](operations.md#tenant-ecs-staging) |

The shared profile starts with authenticated HTTPS intake. Direct tenant
publishing to shared pub/sub is a future transport option that requires
tenant-specific provenance and IAM controls. The current outbound tenant
profile authenticates the receiver over TLS and uses signed callbacks to
authenticate the sender.

## Verification evidence

- The local suite passed **340 tests**, including the LocalStack EventBridge and
  SQS probe. Ruff, all **nine IaC checks**, and the built-container smoke test
  passed.
- The [two-tenant TLS integration test](../tests/integration/test_mtls_transport.py)
  passed five consecutive local runs. Both tenants submitted, read, and
  completed their own jobs. Foreign reads, submissions, completion attempts,
  and artifact references were denied. The test also checked invalid
  certificates, certificate/token mismatch, and direct-origin peer checks.
- In the wall-clock run, the local edge rejected a fresh handshake using the
  revoked certificate within **5 ms** of applying the test CRL. An already-open
  connection was denied by the API **27.54 seconds** after workload deactivation.
  The application failed closed for both tenants on observing the changed
  registry. The unaffected tenant resumed after the API reloaded the registry.
  These readings cover the local proxy and application. Measure AWS propagation
  separately in staging.
- The [MySQL 8.0.46 probe](../deploy/standalone/README.md#local-mysql-tenant-admission-verification)
  passed ownership comparison, concurrency, admission caps, transaction
  rollback, restart, and revocation checks. Generate a fresh receipt when
  repeating the probe.
- With weights of 2 and 1, the higher-weight tenant received about 66.6% of
  charged work in saturated tests. The synthetic burst test measured quiet
  tenant p95/p99 queue waits of 84/88 seconds with weighting, compared with
  2,195/2,199 seconds under FIFO. [Scheduler regression](../tests/unit/test_scheduling_strategy.py)

Run the local release checks from the repository root.

```sh
ruff check src tests deploy/standalone/verify_mtls.py deploy/standalone/verify_mysql_tenant_admission.py
PYTHONPATH=.:src:vendor python -m pytest tests deploy/standalone/test_event_execution.py
bash scripts/check_service_iac.sh
```

CI also needs the repository's read-only GitHub App access to the private
`comms-eventing` Terraform dependency. The prior `develop` run failed at that
credential step. Restore or verify the app settings and rerun the current
revision's hosted checks. [Prior `develop` CI run](https://github.com/ZGTR/foundry-workflow/actions/runs/44334098633)

## What remains before closeout

| Order | Owner role | Required outcome |
| --- | --- | --- |
| 1 | Repository administrator | Confirm hosted CI for the merged revision. Configure the comms read app with contents-only access if the credential check still fails. |
| 2 | Platform operator | Provide a staging AWS deployment role, a dedicated Terraform state and private MySQL ledger, an image digest, and the pinned trust material. Review the tenant profile plan and record the deployed revision. [Staging runbook](operations.md#tenant-ecs-staging) |
| 3 | Platform and service operators | Enroll two test tenants with separate submitter and executor workloads, certificates, tokens, callback destinations, and artifact ACLs. Run live ALB, WAF, IAM, origin isolation, revocation, callback, outbox replay, and storage checks. Record revocation delays on new and established connections and rehearse the registry-driven task replacement. [Isolation release gate](tenant-security.md#release-gate) |
| 4 | Workflow operator | Use real staging executors to measure weighted share and quiet-tenant p95/p99 queue dwell under a burst. Record capacity, policy version, and rollback evidence. Rehearse equal-weight rollback and paused admissions while current attempts finish. [Execution rollout](multi-tenant-execution.md#acceptance-and-rollout) |
| 5 | Release owner | Attach staging evidence to the [verification and rollout issue](https://github.com/ZGTR/foundry-workflow/issues/8). Approve the allowlisted pilot and close the epic after its acceptance criteria are met. |

No AWS staging environment was configured at handover. Keep external tenant
onboarding gated on live isolation evidence, including service and data access
from two independently enrolled workloads.
