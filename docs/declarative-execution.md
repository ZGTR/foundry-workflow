# Declarative execution

WORKFLOW_CATALOG_PATH enables /v2/events and the generic event coordinator.
The catalog is JSON, validated against contracts/workflow-catalog.v2.schema.json.
Start with contracts/examples/catalog.v2.json. v1 HTTP ledger commands and definitions
remain supported, including their default final human-review state.

## Boundaries

- Immutable definition and event models validate contracts; unknown fields fail.
- The execution application service authenticates tenant and producer identity.
- The execution repository port defines atomic intake and recovery operations.
- The SQL adapter joins inbox receipts, definition snapshots, the existing ledger
  reducer, and the existing outbox in one transaction. It performs no network calls.
- The composition root injects catalog, database factory, clock, auth and transport.
- HTTP controllers parse and serialize. SQS and EventBridge adapters own delivery.
- The existing reducer remains the sole authority for stage transitions. No product
  code or product-specific branches belong in this service.

A catalog can include many workflow keys and versions. A matching trigger starts
the highest matching version for each key. Each job stores its complete definition
and uses its digest throughout execution. Restarting with a new catalog cannot
silently change an existing run. Bump the version when editing a definition. Keep
old versions and their completion subscriptions until all old runs have drained.

Stages are sequential. The first receives the trigger's input_ref (or output_ref);
successors receive the previous stage's output_ref. Large inputs and outputs live
in artifact storage; schema identifiers describe the executor contract, not an
instruction for this service to fetch or validate external artifact bytes. Workers
own artifact access and validation. Arbitrary code execution, branching, and parallel
DAG execution are not implied by this contract.

## Event and webhook contract

Publish the workflow.event/v1 envelope in contracts/workflow-event.v1.schema.json.
This is a workflow-owned envelope, not comms.event/v1: the latter is a closed,
comms-specific identifier contract. The common comms eventing kit is envelope-neutral
and reads event_id, type, and producer; workflow supplies all three.

EventBridge Source must equal producer, and DetailType must equal type.
The worker checks both and checks the configured tenant allow-list. Configure
bus publisher IAM so only trusted producers can publish; the queue policy must
admit only the subscribed rules. All producers on a shared ingress are trusted
for its configured tenant set. Use separate deployments/buses for separate trust
boundaries. A tenant string in an unauthenticated HTTP body is never sufficient.

POST /v2/events accepts the same envelope as a webhook. It requires the existing
signed JWT authentication outside local/test/dev. The token subject must match
producer, the tenant must match tenant_id, and jobs:write is required.
HTTP 202 acknowledges a committed inbox transaction, not completion of the work.
The response supplies job IDs; GET /v1/jobs/{job_id}/stages exposes progress.
Use v2 result events to advance event-driven jobs.

A dispatch includes correlation with job ID, stage run ID, definition digest
and attempt fence, plus an expires_at. The external executor must:

1. Persist/deduplicate event_id before acknowledging delivery.
2. Respect expires_at and use a stable business idempotency key for external effects.
   A retry has a new fence/event ID; transport redelivery retains its original ID.
   Use the stage run ID when the same effect must be deduplicated across attempts.
3. Publish the configured success/failure event, echoing correlation unchanged.
4. On success, supply payload_digest and optionally output_ref.
5. On failure, set retryable to true only for transient errors.

Each result's producer and type must match the stage definition. Stale fences,
late results and already finished attempts do not change the ledger. Reusing an
event ID with different content is a conflict. Both event and webhook routes use
the same durable intake. An executor retry signal is a failure with retryable;
the workflow owns the retry budget and due time.

Completion defaults to success and emits job.succeeded. Opt-in review retains
job.review_wait. Stage lease duration is the configured completion timeout,
capped by the job deadline. Recovery ticks turn expired attempts into bounded
retries or terminal failure and expire overdue jobs. No in-process timer is the
source of truth.

## Deployment

The supported complete execution deployment is ECS Fargate with shared MySQL.
The HTTP process and execution worker use the same image and database.
The existing Lambda Web Adapter deployment remains an HTTP ledger deployment;
it is not an SQS consumer. Do not install the one-shot worker as its Lambda CMD.

Set execution_enabled=true in iac/terraform and supply the database, secret ARN,
JWKS/audience, catalog path, output bus and tenant variables. The catalog must
exist inside the immutable image (the image includes contracts/), or be mounted
at the configured path. Use the same catalog bytes to compile subscriptions:

    PYTHONPATH=src:vendor python -m shepherd_workflow_service.subscriptions_main \
      contracts/examples/catalog.v2.json existing-bus > subscriptions.tfvars.json
    terraform -chdir=iac/terraform plan -var-file=/absolute/path/subscriptions.tfvars.json

Supply the deployment's other variables separately. The compiler creates producer-
specific patterns without accidentally combining unrelated producers and types.
The pinned comms consumer_queue module supplies queues, DLQs, alarms and subscription
policies. CI needs read access to ZGTR/comms-eventing. Review rule replacement when
changing subscription keys: the shared module treats its first subscription positionally.
Products own their dispatch subscriptions; this deployment owns trigger/result subscriptions.

The ECS execution worker runs python -m shepherd_workflow_service.execution_main.
Every cycle long-polls SQS, commits successful deliveries before deleting messages,
recovers due attempts, and drains the existing ledger outbox. Poll failures back off;
the ECS service restarts crashed workers. Queue retries and DLQ handling are distinct
from workflow attempts. Replay DLQ events with their original envelope event IDs.

The EventBridge adapter checks the bus before publishing and checks per-entry errors.
Publish failures leave outbox rows pending. A crash after publish but before marking
sent can redeliver the same event. Consumers must remain idempotent. Deleting a bus
during a send is still a transport administration failure; retain/replay outbox history
and restore rules before replay, following the comms recovery runbook.

The added execution binding/inbox tables are created alongside the existing ledger;
no existing column changes are required. For managed databases, provision them in a
release migration before restricting runtime DDL privileges. Retain inbox receipts
for at least the full replay window; removing them permits an old trigger to run again.

## Verification

The integration suite uses real SQLite transactions, clock-controlled retries,
restart reconstruction, HTTP ingress, outbox publication failures and stale-result
checks. The container and MySQL probes in deploy/standalone cover the original
ledger's auth, durability and concurrency guarantees. Cloud deployment and an actual
product executor remain separate release checks; local tests do not claim AWS
delivery or downstream exactly-once side effects.

Operational metrics, tenant-scoped history, recovery checks and the staged ECS rollout
are documented in [operations.md](operations.md).
