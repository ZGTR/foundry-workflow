# Operating declarative execution

The execution worker emits CloudWatch Embedded Metric Format through its existing
log stream. No `cloudwatch:PutMetricData` grant is needed. Metrics use only
`Service=foundry-workflow` and `Environment`; tenants, job IDs and artifact references
never become metric dimensions. When execution is enabled, Terraform creates a
fleet dashboard and alarms for missing heartbeat, cycle failures, pending outbox
age and overdue recovery. Supply `execution_alarm_actions` SNS ARNs to enable paging.
The shared comms consumer module also owns queue/DLQ monitoring.

`GET /v2/jobs/{job_id}/history?limit=100` requires a signed `jobs:read` principal for
the job's tenant. It returns the newest recorded outbox events, their stable IDs,
correlation fields and publication status. The maximum limit is 500. Artifact
contents and references are excluded. This is a bounded delivery history, not a
complete audit export: inbound rejected events and duplicate deliveries are not
outbox events. Outbox retention determines available history.

## Responding to alarms

- **Heartbeat missing:** inspect ECS worker desired/running counts, task exit reason
  and logs. Verify catalog, shared MySQL and queue/bus configuration. A live API does
  not prove that its separate worker is running.
- **Worker cycle failures:** inspect the logged operation (`consume`, `recover`,
  `publish`) and SQS message failures. Intake, timeout recovery and publishing run
  independently; a failed intake no longer prevents due jobs from progressing.
- **Outbox age:** verify EventBridge reachability, `DescribeEventBus`/`PutEvents`
  permissions and throttling. Leave unsent rows in place. The worker retries their
  stable event IDs; do not mark them sent manually.
- **Overdue stages:** verify database health, worker heartbeat and recovery logs.
  The metric includes elapsed leases, elapsed retry delays and job deadlines.
- **DLQ backlog:** fix authentication, schema, selector or database failures first.
  Redrive to the original ingress queue using the comms operations procedure.
  Preserve the original event envelope and event ID. Successful redelivery is
  idempotent; modifying content under an existing ID causes an identity conflict.

`RetryAttempts` is a cumulative count retained in the ledger, not a per-minute
rate. `FailedStages`/`ExpiredStages` likewise count retained stage records.
Metrics currently query the ledger once per worker cycle; large ledgers should
measure query cost before reducing polling intervals or increasing worker count.

## Local transport and recovery verification

```sh
pip install -e '.[aws,jwt,mysql,dev]'
docker pull localstack/localstack:3.8
PYTHONPATH=.:src:vendor python -m pytest -v -s deploy/standalone/test_event_execution.py
```

This opt-in test launches a bounded, isolated LocalStack container with dummy AWS
credentials, exercises the production EventBridge broker and SQS consumer against
its actual HTTP APIs, and removes its container afterward. It verifies two product
profiles configured only with JSON, duplicate intake/results, durable executor
idempotency after a simulated crash, workflow reconstruction, transient failure,
timeout recovery, stale fencing and SQS DLQ replay. The executor's side-effect key
is committed in its own SQLite database. The ordinary integration suite separately
checks an ambiguous/outage publication with stable retry IDs, transaction rollback
and concurrent duplicate intake. `verify_mysql.py` covers the shared MySQL ledger.

These are local emulator and database checks. They do not establish AWS IAM/network
correctness or claim integration with a live product. An external worker must
implement its own durable idempotency, authenticate its producer identity and
return the exact dispatch correlation. Workflow retries cannot make an arbitrary
non-idempotent external effect execute exactly once.

## CI access to the private comms module

The shared Terraform module stays pinned in `ZGTR/comms-eventing`; its private
source is not copied into this repository. Install a GitHub App on that repository
with **Contents: read** only. Configure repository variable `COMMS_READ_APP_ID`
and Actions secret `COMMS_READ_APP_PRIVATE_KEY`. The local composite action mints
a short-lived token restricted to that repository and supplies a URL-scoped Git
header for Terraform. The token action revokes it after the job. Never use a
personal session token as a permanent CI secret.

Fork pull requests cannot read repository secrets. Run the IaC job on a reviewed,
trusted branch; do not change it to `pull_request_target` with untrusted checkout.
Missing credentials fail explicitly. Lint/tests and local transport checks remain
independent of the private-module credential.

## Staging rollout

Use **Staging execution rollout**, not the older Lambda deployment job, for the
continuous execution worker. It defaults to plan-only and runs only on manual
dispatch. Configure the GitHub `staging` environment and an AWS OIDC trust policy
restricted to this repository/environment. Required settings:

| Setting | Purpose |
| --- | --- |
| `DEPLOY_ROLE_ARN` secret | Environment-scoped deployment role |
| `AWS_REGION` variable | ECR, Terraform and ECS region |
| `TF_STATE_BUCKET`, `TF_STATE_LOCK_TABLE` variables | Existing encrypted S3 backend and DynamoDB state lock |
| `WORKFLOW_TFVARS_JSON` variable | Terraform configuration described below |
| `SERVICE_BASE_URL` variable | HTTPS hostname covered by the configured ACM certificate |
| `SMOKE_BEARER_TOKEN` secret | Current signed token with `jobs:read` for smoke checks |
| Comms app settings above | Private module read access |

The JSON configuration supplies `environment: "staging"`, `aws_region`, `vpc_id`,
`certificate_arn`, `workflow_catalog_path`, `workflow_event_bus_name`,
`workflow_event_bus_arn`, `workflow_event_tenants` (array), `db_host`, `db_port`,
`db_name`, `db_user`, `db_password_secret_arn`, `auth_jwks_uri`, `auth_audience` and
optional `execution_alarm_actions`. Never put a database password in it. The VPC
needs at least two suitable public subnets tagged `Tier=public` for the internet
ALB and private subnets tagged `Tier=private` for tasks, private database access,
and NAT or appropriate VPC endpoints for AWS services. Configure DNS for the
service hostname. The task role must also have KMS decrypt permission if its
Secrets Manager secret uses a customer-managed key that requires that grant.

Supply an already-built immutable private ECR image digest. The workflow extracts
the catalog **from that image**, validates it, compiles subscriptions, initializes
`foundry-workflow/staging/terraform.tfstate`, and plans. It does not accept an
independently edited subscription list. Plan-only does not change infrastructure.
Selecting `apply` applies the saved plan, waits for both ECS services, verifies
that a circuit-breaker rollback did not silently replace the intended revisions,
and checks SQL readiness, unauthenticated denial and authenticated tenant lookup.
A smoke failure fails the workflow; it does not claim to undo database changes.
To revert an application revision, dispatch the previous tested image digest and
review its plan. Do not delete ledger/outbox rows during rollback.

The partial S3 backend requires explicit initialization. Existing local state must
be migrated deliberately with the correct environment's bucket/key, never
silently replaced. CI validation uses `-backend=false` and needs no AWS account.

After the rollout, perform a real product canary: publish one authorized request,
observe dispatch at the product's executor, return its result, and confirm the
terminal event and history. Repeat with a second independently configured product.
The generic local profiles prove the configuration boundary; they are not a
substitute for those live canaries.
