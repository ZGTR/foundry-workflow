# foundry-workflow — runbook

## Run it locally

```bash
PYTHONPATH=.:src:vendor \
SERVICE_ENV=local \
PORT=8080 \
DB_PATH=/tmp/shepherd-workflow.db \
WORKFLOW_DEFINITION_PATH="$PWD/contracts/workflow.v1.json" \
python -m shepherd_workflow_service.main
```

Create a virtualenv first (see `CONTRIBUTING.md`). `PYTHONPATH=.:src:vendor`
stands in for installing the packages — `.` because the tests import
`tests.conftest`, `src` for the service, `vendor` for `shepherd_workflow` and
`shepherd_contracts`.

```bash
# the suite
PYTHONPATH=.:src:vendor .venv/bin/python -m pytest tests -q

# lint
.venv/bin/ruff check src tests
```

Run the suite from a clean checkout, one process at a time.

## Smoke it

```bash
curl -s localhost:8080/healthz          # {"status":"up"}
curl -s localhost:8080/readyz           # {"ready":true}

H=(-H 'x-org-id: org_1' -H 'x-subject-id: user_1'
   -H 'x-scopes: jobs:write,jobs:read' -H 'content-type: application/json')

curl -s -X POST localhost:8080/v1/jobs "${H[@]}" \
     -d '{"job_id":"job_1","input_digest":"dig_1"}'

curl -s -X POST localhost:8080/v1/stage-runs/<srun_id>/commands "${H[@]}" \
     -d '{"kind":"lease","state_version":0}'
```

`x-scopes` is only trusted because `SERVICE_ENV=local`. See "Auth" below.

## Configuration

`config.py` is the only module in `src/` that reads the environment; everything
else takes its inputs explicitly. `tests/shape/test_layering.py` enforces that
by parsing the AST rather than grepping, so a docstring may name `os.environ`
without tripping it.

| Variable | Default | What it does |
| --- | --- | --- |
| `SERVICE_ENV` | `local` | `local`, `test` and `dev` permit the unverified header auth adapter. Anything else is treated as production — an unrecognised name is secure by default |
| `PORT` | `8080` | uvicorn's bind port |
| `PRINCIPAL_ISSUER` | `foundry-workflow` | the issuer stamped on minted principals |
| `RUNTIME_MODE` | derived | `lambda` when `AWS_LAMBDA_FUNCTION_NAME` is set, else `http`. Nothing branches on it today |
| **`WORKFLOW_DEFINITION_PATH`** | **none — REQUIRED** | the JSON workflow this deployment runs. No default; the service refuses to start without it |
| `WORKFLOW_TABLE_PREFIX` | `wf_` | the PHYSICAL prefix on this deployment's five tables. Changing it points the service at a different ledger |
| `WORKFLOW_JOB_DEADLINE_SECONDS` | `3600` | how long a stage run has before `expire` may be accepted against it |
| `DB_BACKEND` | `sqlite` | `sqlite` or `mysql`. Anything else is refused at startup |
| `DB_PATH` | `workflow.db` | SQLite file. Ignored on MySQL |
| `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` | none | required together when `DB_BACKEND=mysql` |
| `DB_PASSWORD_SECRET_ID` | none | the NAME of the secret holding the password. There is deliberately no `DB_PASSWORD`: a password in the environment is readable by every child process and printed by every crash dump that dumps the environment |
| `AUTH_JWKS_URI` / `AUTH_AUDIENCE` | none | required together outside a local env |
| `EVENT_BUS_MODE` | `in_process` | `in_process`, `sqs` or `outbox`. `sqs` requires `SQS_QUEUE_URL` |
| `SQS_QUEUE_URL` / `AWS_REGION` | none | for the SQS publisher and the outbox drain |

### `WORKFLOW_TABLE_PREFIX` is a pointer, not cosmetics

It names the physical tables AND the write-guard's permitted set, from one
value. Changing it on a running deployment does not rename anything: the service
starts, creates a fresh set of tables under the new prefix, and the existing
ledger becomes invisible. Treat a change as a migration, not a config tweak.

## The three refusals you will be paged about

`/readyz` and the logs will not tell you these; a caller's 409 will.

| `detail` | What actually happened | What to do |
| --- | --- | --- |
| `state_version_conflict` | two callers acted on the same version; one lost | the loser re-reads and retries. Persistent recurrence means two workers are racing for the same stage — check for a duplicated consumer |
| `stale_fence` | a worker whose lease was superseded came back with an old fence | correct behaviour, not a bug: its write was rejected on purpose. Investigate why the first worker was slow enough to be superseded |
| `lease_expired` | a worker held a lease past its expiry and then tried to complete | the work took longer than the lease. Either the worker is stuck or the lease duration is too short for that stage |

`workflow_digest_mismatch` (422) is different in kind: the caller is talking
about a workflow this deployment does not run. Either the definition file was
changed under a running service, or two deployments are pointed at one ledger.

## Startup refusals

These kill the process at `build_container()`, before it serves anything, with
the fix in the message. That is the intended behaviour: the alternative to
"insecure" is not "green", it is "insecure and green".

| Message contains | Cause | Fix |
| --- | --- | --- |
| `WORKFLOW_DEFINITION_PATH is not set` | no workflow | point it at a JSON definition |
| `unknown db_backend=` | a typo in `DB_BACKEND` | use `sqlite` or `mysql`. There is no fall-through |
| `needs a SecretsPort` | `DB_BACKEND=mysql` with no secrets adapter wired | wire one; do not set a `DB_PASSWORD` |
| `db_backend='mysql' requires [...]` | a missing MySQL field | supply the named fields |
| `InsecureDefaultAuth` | non-local `SERVICE_ENV` with no `AUTH_JWKS_URI`/`AUTH_AUDIENCE` | set both, or set `SERVICE_ENV` to a local value if the run genuinely is local |

## Auth

Locally, `HeaderPrincipalAuth` reads `x-org-id`, `x-subject-id` and `x-scopes`
and verifies no signature. Under it, any caller is any tenant with any scope.

Outside `{local, test, dev}` that adapter is refused. A service that forgot to
supply real auth dies at startup rather than serving traffic while authorising
every request — which is the failure this check exists to prevent, because it is
silent.

## Readiness

`/healthz` answers "is this process up" and needs nothing. `/readyz` asks the
workflow engine for a stage run that does not exist — a single indexed SELECT —
and answers `false` on any exception, with a 503.

The probe principal carries no scopes and an org id no tenant can hold, so its
only two possible outcomes are "not found" and an exception. It cannot read a
tenant's row even if the probe id collided with a real one.

If `/readyz` is false: the ledger is unreachable. Check the database, not the
service.

## Deploy

Not yet done from this repository, and the honest state is in
`blackboxes/workflow-ledger/proof.md` under "What is NOT proven here": nothing
has been applied to an AWS account, `iac/` was not exercised, and GitHub Actions
are paused for this repository.

`iac/serverless.yml` and `iac/terraform/` describe the two targets — a Lambda
image with the AWS Lambda Web Adapter, and ECS Fargate — and both run the same
`docker/entrypoint.sh`. `scripts/check_service_iac.sh` is the validation gate
(`terraform validate` plus `serverless print`); it needs `tofu`/`terraform` and
`npx` and was not run here.

## Roll back

The service is stateless; the ledger is not. A rollback is an image rollback,
and it is safe only while the schema is unchanged — `metadata.create_all` is
additive and `checkfirst`, so an older image against a newer schema will run and
simply not use the new columns. An older image against a schema whose columns
were REMOVED will not. Nothing in this repository removes a column today.

Do not "roll back" by pointing at a different `WORKFLOW_TABLE_PREFIX`. That is
not a rollback; it is a fresh, empty ledger with the old one abandoned.
