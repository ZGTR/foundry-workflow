# foundry-workflow

A durable workflow ledger served over HTTP: **one state machine, two kinds of
caller.** A job runner submits commands against a stage run, and a multi-agent
orchestration submits the same commands against the same ledger. They are not
two systems: splitting them would put a second copy of the lease / fence /
idempotency core somewhere only one of the two is tested. See
[`docs/decisions/0001-one-service-for-jobs-and-orchestration.md`](docs/decisions/0001-one-service-for-jobs-and-orchestration.md).

The service is a FastAPI application over SQLAlchemy Core, with SQLite for local
runs and MySQL 8 for deployments. One image runs as an **AWS Lambda (image
package type)** or an **ECS Fargate task**: one Dockerfile, one ECR repository,
two thin IaC wrappers under `iac/`.

`vendor/shepherd_workflow` holds the reducer and SQL package the service runs,
and `vendor/shepherd_contracts` the primitives it shares. They ship inside the
image and the wheel; they are not separate distributions.
`vendor/workflow-provenance.json` pins the exact hashes of the workflow package,
and `tests/contract/test_workflow_provenance.py` refuses a tree that does not
match.
Applications call the HTTP API and do not copy the engine.

## The contract, in four sentences

- A stage run advances only from the state version the caller says it read. Two
  callers at the same version: one wins, the other is refused
  `state_version_conflict` and the ledger ends one version on, not two.
- A refusal is an **answer**, not a fault. It arrives as 409 or 422 carrying the
  refusal class verbatim, never a 5xx, and a refused command writes nothing —
  the transaction is left by raising, so it rolls back.
- Another org's stage run is **404, not 403**. A stage run id is derived from a
  dispatch key, so a distinguishable answer would let one tenant enumerate
  another's jobs without reading one.
- Starting the same job over the same input twice returns the **same** stage
  run. The database's unique index on the dispatch key resolves it, not a
  check-then-insert.

Full contract and schemas: [`blackboxes/workflow-ledger/`](blackboxes/workflow-ledger/).

## Quick start

Requires Python 3.12 or newer.

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[aws,jwt,mysql,dev]" ruff pytest

# the suite and the linter
PYTHONPATH=.:src:vendor python -m pytest tests
ruff check src tests

# run the real process, exactly as docker/entrypoint.sh execs it
SERVICE_ENV=local \
WORKFLOW_DEFINITION_PATH=contracts/workflow.v1.json \
DB_PATH=/tmp/workflow.db PORT=8080 \
PYTHONPATH=src:vendor python -m shepherd_workflow_service.main
```

In another shell:

```bash
H='-H content-type:application/json -H x-org-id:org_1 -H x-subject-id:user_1 -H x-scopes:jobs:write'

curl localhost:8080/healthz
curl -X POST localhost:8080/v1/jobs $H -d '{"job_id":"job_1","input_digest":"dig_1"}'
# -> 201 {"stage":"plan","status":"pending","state_version":0,...}

curl -X POST localhost:8080/v1/stage-runs/<id>/commands $H \
  -d '{"kind":"lease","state_version":0}'
# -> 200 {"status":"leased","state_version":1,"fence":1,...}

# the same command again, at the version it already left behind
curl -X POST localhost:8080/v1/stage-runs/<id>/commands $H \
  -d '{"kind":"lease","state_version":0}'
# -> 409 {"detail":"state_version_conflict"}
```

That 409 is the assertion worth writing a probe around: only a service that
actually read its ledger can produce it. The `x-org-id` / `x-subject-id` /
`x-scopes` headers are the development identity; outside
`SERVICE_ENV=local`/`test`/`dev` the service refuses them and requires signed JWTs
(`AUTH_JWKS_URI`, `AUTH_AUDIENCE`).

To build and run the container image:

```bash
docker build -t workflow:local .
```

`deploy/standalone/` holds a Compose deployment and two probes that verify a
built image end to end (signed auth, restart, backup/restore, and a MySQL 8
ledger run). See [`deploy/standalone/README.md`](deploy/standalone/README.md).

## Testing

`PYTHONPATH=.:src:vendor python -m pytest tests` runs the whole suite: unit,
integration (real SQLite, real HTTP process), contract and shape tests. Tests
that need Docker or MySQL are the probes in `deploy/standalone/`, run by hand.
`bash scripts/check_service_iac.sh` validates the Terraform and Serverless
configuration without touching an AWS account; it needs `terraform` and `npx`
on `PATH`. [`CONTRIBUTING.md`](CONTRIBUTING.md) lists the checks to run before
a pull request.

## Configuration

`config.py` is the ONE module allowed to read the environment; every other
module receives a `ServiceConfig` through a constructor, and
`tests/shape/test_layering.py` asserts it.

| Variable | Default | What it decides |
| --- | --- | --- |
| `WORKFLOW_DEFINITION_PATH` | *(none — refuses to start)* | The JSON workflow this deployment runs. No built-in default: a service with no workflow accepts jobs it can never advance. |
| `WORKFLOW_TABLE_PREFIX` | `wf_` | The ledger's physical table names AND the set the package's write-guard permits. ONE value reaches both. |
| `WORKFLOW_JOB_DEADLINE_SECONDS` | `3600` | How long a stage run has before `expire` may be accepted. |
| `DB_BACKEND` | `sqlite` | `sqlite` or `mysql`. An unrecognised value is refused at startup, never defaulted. |
| `DB_PATH` | `workflow.db` | SQLite file. |
| `DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD_SECRET_ID` | — | MySQL. The secret's **name**, never the password: a password in the environment is readable by every child process. |
| `AUTH_JWKS_URI` / `AUTH_AUDIENCE` | — | Real auth. Without them, any `SERVICE_ENV` other than `local`, `test` or `dev` refuses to start rather than falling back to unverified headers. |
| `PRINCIPAL_ISSUER` | `foundry-workflow` | The issuer stamped on minted principals and required of signed tokens. Set it explicitly in every deployment; the default changed when the service took its current name. |

**The prefix rule is the one to read twice.** `build_tables(prefix=P)` names the
tables and `owned_tables(P)` names the set the write-guard permits, and both take
`config.workflow_table_prefix`. A literal `wf_` set would be correct only at the
default: a deployment on `app_` would create `app_jobs` and then be refused it
by the guard. `tests/integration/test_sql_workflow_engine.py` runs three
prefixes, because the `wf_` arm stays GREEN under that mutation.

## Layout

```
src/shepherd_workflow_service/
  ports/workflow_engine.py        what the HTTP layer needs FROM the engine
  adapters/real/sql_workflow_engine.py    the package, over a real database
  adapters/fake/workflow_engine.py        the same runner, over a dict
  models/workflow_command.py      the one request -> CommandV1 translation
  services/workflow_service.py    refusal -> 404 / 409 / 422
  controllers/workflow.py         parse, authenticate, call, serialise
vendor/shepherd_workflow/         the pinned legacy package with recovery patch
blackboxes/workflow-ledger/       contract, schemas, proof
docs/                             architecture, runbook, decisions
```

The engine port faces the opposite way from the package's own ports, and that is
deliberate: `StageRepository` and `OutboxSink` are what the package needs from
its host; `WorkflowEnginePort` is what the HTTP layer needs from the engine.

## Status

The service, its SQLite and MySQL ledgers and the container image are verified
locally: the suite passes, and the probes in `deploy/standalone/` exercise a real
image with signed authentication, restart and restore, and a real MySQL 8 ledger
covering concurrent claims and persisted retry limits. Cloud deployment through
`iac/` has not been verified, and the GitHub Actions workflow is currently
paused. [`blackboxes/workflow-ledger/proof.md`](blackboxes/workflow-ledger/proof.md)
records the original evidence, with the command beside each figure.

## Security and contributing

Report vulnerabilities as described in [`SECURITY.md`](SECURITY.md).
Contributions are welcome; see [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

MIT. See [`LICENSE`](LICENSE).
