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

## Tenant ECS staging

Use the [service mTLS guide](service-mtls.md) to provision the client trust
material and run the staging certificate verifier.

Use a dedicated Terraform state key and resource name for tenant ECS. Keep the
legacy deployment on its own ledger. Provision a separate private MySQL ledger
and database user for tenant ECS. Restrict database ingress to the tenant API and
worker security groups and grant that user access only to its ledger. The
Serverless stack runs on Lambda-local SQLite and has no grant to the tenant
database. Remove any older consumer, internal route, credential or deployment
that can reach the ledger before enrolling tenants.

Supply an immutable ECR image with its validated catalog at
`workflow_catalog_path`. Use `tenant_profile = true` and
`execution_enabled = true` with the normal region, VPC, ACM certificate, MySQL
and OAuth audience/JWKS settings. Leave the legacy EventBridge, event-tenant,
SQS and subscription inputs at their empty defaults. The module provisions the
worker's output bus with a policy denying publication outside the worker role.
Keep output routing under operator control. Do not add rules that deliver the
shared output to a tenant-controlled EventBridge or SQS target.

Configure `tenant_gateway` with these inputs.

| Setting | Purpose |
| --- | --- |
| `alb_subnet_ids` | Public ALB subnets in at least two availability zones in the same VPC. Their private CIDRs are the API's trusted immediate socket-peer ranges. |
| `trust_store_bucket`, `ca_bundle_key`, `ca_bundle_object_version` | Operator-managed S3 CA bundle pinned to an object version |
| `revocation_list_key`, `revocation_list_object_version` | Pinned issuer CRL in the same bucket |
| `auth_issuer`, `token_profile` | Exact HTTPS token issuer and profile. Default to `rfc9068`. Use `cognito` only for a staging-verified provider profile. |
| `registry_secret_arn` | Secrets Manager ARN for the workload and grant registry JSON |
| `backend_tls_secret_arn` | Secrets Manager ARN for a JSON object with `certificate` and `private_key` PEM strings |
| `pool_capacity`, `artifact_prefixes` | Optional scheduler capacity and map of tenant IDs to permitted artifact URI prefixes |
| `queue_age_seconds` | Optional age at which an eligible head can use the overdue admission turn. Defaults to 60 seconds. |
| `callback_registry_secret_arn`, `callback_signing_secret_arns` | Optional callback registration JSON secret and the exact signing secret ARNs it references |

Store the secrets outside Terraform. For customer-managed KMS keys, scope
`kms:Decrypt` to each secret's key and the workload roles that read that secret.
The worker alone needs callback registration and signing key access. Each
registration supplies `tenant_id`, `destination_id`, `version`, `event_types`,
`url`, `key_id` and `secret_id`. Use the signing secret ARN as `secret_id`.
Configure the private subnets with routing to MySQL, Secrets Manager, EventBridge,
ECR and CloudWatch Logs. Provide egress to the configured JWKS and approved
callback endpoints. Use an ACM certificate and DNS name that clients can verify.

Initialize the partial S3 backend with the dedicated key and run
`terraform plan -var-file=tenant-staging.tfvars -out=tenant-staging.plan`.
Apply the reviewed plan only in the authorized staging account. The existing
**Staging execution rollout** workflow compiles legacy queue subscriptions
and targets the legacy staging state. Use the dedicated tenant plan for this
profile.

The ALB verifies client certificates against the trust store and CRL. WAF
rejects client-supplied `X-Amzn-Mtls-*` headers and bounds header inspection.
The API accepts one ALB-generated URL-encoded leaf certificate and verifies the
OAuth token against that leaf and the workload registration. The listener and
WAF allow only `POST /v2/events`, `GET /v2/stage-runs/{stage_run_id}`,
`GET /v2/jobs/{job_id}/stages` and `GET /v2/jobs/{job_id}/history`. The target
group runs readiness over its private HTTPS connection.

AWS ALB encrypts HTTPS connections to targets and does not validate their
server certificates. The application origin is protected by the private task
network, source security group and socket-peer check. Keep permissions to
attach the ALB's security group or edit its listener and WAF limited to the
deployment role. [AWS HTTPS target group behavior](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-target-groups.html#target-group-routing-configuration)

Before sending tenant traffic, run the tenant release gate in staging. Confirm
that both ECS services run the planned revisions and that the target is healthy.
Use separate client certificates and tokens for two tenants to exercise their
allowed routes, missing and foreign reads, and private-origin denial from a
separate VPC workload. Confirm rejection of a missing, expired or revoked
certificate, a token bound to a different leaf, and caller-chosen leaf headers
with duplicate, mixed-case, empty and oversized inputs. Verify a correct client
without a supplied leaf header succeeds through WAF. Verify `/readyz`, OpenAPI,
legacy writes and raw lease routes are inaccessible at the edge. Measure
registry deactivation and CA/CRL updates through to rejection and record the
observed delay. Recheck client certificates with a fresh connection and with a
previously opened connection. Record callback, recovery and MySQL ownership
evidence from the [tenant release gate](tenant-security.md#release-gate)
before onboarding independently trusted tenants.

### Initialize admission and enroll workloads

Use an operator task based on the same image and tenant configuration. Give it
private database access and the required schema migration grant for cutover.
Stop writers from older images before running enable on a retained ledger.
Keep the tenant API dark until the operator reports success. MySQL applies
column changes individually. If interrupted, repeat the migration after
checking the database. The command audits all owners before adopting exact
comparison and stops on inconsistent data without changing ownership. Back up
the ledger before upgrading it.

Supply a policy file with all admitted tenants. The initial example uses two
capacity shares, a queue allowance of 100 jobs per tenant, and up to four
concurrent attempts per tenant.

```json
{
  "tenants": {
    "tenant-a": {
      "weight": 2, "backlog_cap": 100, "in_flight_cap": 4,
      "version": 1, "actor": "operator"
    },
    "tenant-b": {
      "weight": 1, "backlog_cap": 100, "in_flight_cap": 4,
      "version": 1, "actor": "operator"
    }
  }
}
```

```sh
python -m shepherd_workflow_service.tenant_admin_main enable /run/workflow/policy.json --mode weighted
python -m shepherd_workflow_service.tenant_admin_main metrics
```

Optional stage cold-start costs use the file's `stage_cost_seconds` object,
mapping an immutable definition digest to an object of stage keys and positive
integer seconds. Use a single-writer operator task for policy changes. Apply an
individual versioned policy using `policy <tenant> <policy-file>` with one
policy object as the file. For rollback, use `mode equal` to retain admission
and tenant isolation with uniform weights. Use `mode paused` to stop opening
new work while existing attempts finish and the outbox drains. Keep earlier
immediate-dispatch binaries offline throughout rollback.

Set `queue_age_seconds` in the tenant gateway configuration or use
`TENANT_QUEUE_AGE_SECONDS` in a standalone deployment. Valid values are whole
seconds from 1 through 604,800. Apply the same setting to all admission workers.
The threshold makes an eligible job a candidate for the bounded overdue turn.
Actual dwell time also depends on available slots, lease durations and backlog.

Register the OAuth issuer, client ID, subject, tenant ID, producer, permitted
scopes and SHA-256 leaf certificate thumbprints for each workload. Thumbprints
use unpadded base64url of the certificate DER digest. Include `active: true` on
enrolled workloads. Authorize the exact workflow digest and event type in
`triggers` and authorize the exact executor client, digest and stage in
`executors`. Replace tasks after changing the versioned registry so every
process uses the same snapshot. An existing process stops accepting the old
workload grants when its refresh detects a change. Rotate client certificates
with an overlap only for the same workload.

Clients establish mTLS and pass a short-lived access token as
`Authorization: Bearer …`. The certificate is presented during TLS. ALB supplies
the verified leaf context to the private origin. Clients submit the
[workflow event envelope](../contracts/workflow-event.v1.schema.json) by
`POST /v2/events`, preserve the event ID on an identical retry, and keep
separate registered submitter and executor credentials. A 202 returns durable
job IDs. A 429 supplies a Retry-After. Poll the v2 stages and history endpoints
using a read-scoped credential. An external executor receives the dispatch
through its registered callback, verifies the signature and expiry, and
reports a result that echoes the correlation and fence. Recovery still runs
after a callback delivery failure. A receiver must finish or cancel by the
lease expiry and deduplicate effects across retries.

Keep artifact namespaces assigned permanently to one tenant and configure
each executor role for only its permitted paths. The service authorizes
registered S3 references and does not fetch or grant access to object bytes.
Provision least-privilege storage access separately. Never reassign an
artifact prefix to a different tenant after offboarding.

For signed callbacks, use a distinct Secrets Manager key per tenant with a
base64-encoded key of at least 32 random bytes. The receiver validates
RFC 9421 HMAC-SHA256, its configured key ID, the target, method, request
content digest, creation and expiry, and stable event ID. Install a new
verification key before rotating the signing registration. Callback policy
is rechecked before each send. Restart the worker after an authorized routing
change. Use the operator `replay <tenant> <event-id>` command to reevaluate
quarantined deliveries for the original owner after addressing their failure.
