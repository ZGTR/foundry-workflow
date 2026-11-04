# Blackbox: workflow ledger — contract v1

**Promise.** One place in the estate holds the durable state of long-running
work — a job, its stages, who currently holds each one, and what has already
happened to it — and it moves that state forward only when the caller can show
it is acting on the state it actually read. A caller learns that its command was
applied, or it learns the named reason the ledger declined it, or it learns that
this service failed and decided nothing. Those three outcomes are never
confusable.

The same ledger serves both callers this estate has: a durable JOB queue, and
multi-agent workflow ORCHESTRATION. They are the same state machine with
different callers, which is why they are not two services
(`docs/decisions/0001-one-service-for-jobs-and-orchestration.md`).

## Surface

Three routes. See [`schema/stage_run.v1.json`](schema/stage_run.v1.json) and
[`schema/command.v1.json`](schema/command.v1.json).

```
POST /v1/jobs                              -> 201  a stage run
POST /v1/stage-runs/{id}/commands          -> 200  a stage run
                                              409/422  a refusal, named
GET  /v1/stage-runs/{id}                   -> 200  a stage run
                                              404  not visible to you
```

```
POST /v1/jobs   {"job_id": "job_1", "input_digest": "dig_1"}
    ->  201  {"stage_run_id": "srun_...", "job_id": "job_1",
              "workflow": "shepherd.agent_run.v1", "stage": "plan",
              "status": "pending", "state_version": 0,
              "fence": null, "attempts": 1}
```

The org is taken from the authenticated principal
(`CallerPrincipalV1.org_id`), never from the body. A caller that could name its
own org could read — and advance — another tenant's work.

## What goes in

| | |
| --- | --- |
| `job_id` | the caller's own id for the job. Half of the dispatch key |
| `input_digest` | a digest of the input the job runs over. The other half |
| `kind` | the command: `lease`, `complete`, `fail`, `cancel`, `expire`, `start` |
| `state_version` | the version the caller READ. Never optional, never defaulted |
| `fence` | the lease fence this caller was granted. Null before a lease |
| `payload` | per-command fields the reducer reads — `payload_digest`, `provider_pin`, `dispatch_key`, `reason` |
| the principal | org, subject id and scopes, from the request's authentication, not the body |

`job_id`, `stage_key` and `workflow_digest` are **not** inputs to a command.
They come from the stored stage run. A caller restating them can only ever
restate them wrongly, which turns a valid request into a refusal and is never a
way to say anything new.

## What comes out

Always the same object on success — `stage_run_id`, `job_id`, `workflow`,
`stage`, `status`, `state_version`, `fence`, `attempts` — for all three routes.
Reading a stage run back after a command returns exactly what the command
returned.

On a refusal, a 4xx whose `detail` is the refusal class **verbatim**. Match on
that string; it is the stable, machine-matchable half of the answer.

## What it guarantees

Each row names the symbol that enforces it and the test that would go red.
Symbols, not line numbers — a line number is wrong by the next commit, and a
citation that cannot be checked reads exactly like one that was.

| Guarantee | Enforced by | Observable as |
| --- | --- | --- |
| A stage run advances only from the state the caller says it READ | `SqlAlchemyStageRepository.apply`'s single conditional `UPDATE ... WHERE state_version = :expected`, reached through `SqlWorkflowEngine._apply`; the version comes from the request, never from the snapshot (`models/workflow_command.build_command`) | two callers submitting at the same `state_version` produce one 200 and one 409 `state_version_conflict`, and the final `state_version` is 1 — `test_two_callers_at_the_SAME_state_version_do_not_both_win` |
| A refusal is an ANSWER with a named class, never a fault | `WorkflowService.submit` maps `CommandOutcome.refusal` through `_CONFLICT_REFUSALS` / `_NOT_FOUND_REFUSALS` to `StageRunConflict` / `StageRunNotFound` / `StageRunRejected`, and `app_factory._STATUS_BY_ERROR` gives each a 4xx | `409` with `detail` equal to the class, e.g. `state_version_conflict` — `test_a_stale_state_version_is_409_naming_the_refusal_class` |
| A refused command writes NOTHING | `SqlWorkflowEngine._apply` raises `_Refused` from inside `connection.begin()`, so the transaction rolls back; the outbox writes happen in that same transaction | after a refused command the outbox table is empty, so no event was published for a transition that never happened — `test_a_refusal_writes_nothing` |
| An unrecognised refusal class is a 422, not a silent 409 | the fall-through `raise StageRunRejected(refusal)` in `WorkflowService.submit` | a class the package adds surfaces as "retrying will not help" rather than as advice to retry forever |
| Another org's stage run is INVISIBLE, not forbidden | `SqlWorkflowEngine.get` returns `None` when `_org_of(...) != principal.org_id`, and `WorkflowService.get` turns `None` into `StageRunNotFound` | `404`, identical to a stage run that does not exist — `test_another_orgs_stage_run_is_invisible_rather_than_forbidden` and `test_another_orgs_stage_run_is_404_not_403` |
| Starting the same job over the same input twice is ONE stage run | the id is `derive_id("srun", dispatch_key)` and the `dispatch_key` column carries a UNIQUE index; `start_job` catches `IntegrityError` and reads the winner's row | two `POST /v1/jobs` return the same `stage_run_id` at `state_version` 0 — `test_starting_the_same_job_over_the_same_input_twice_is_one_stage_run` |
| The physical tables this service CREATES are exactly the ones it is allowed to WRITE | `SqlWorkflowEngine.__init__` passes ONE `table_prefix` to both `build_tables(prefix=...)` and `owned_tables(...)` | a deployment on `WORKFLOW_TABLE_PREFIX=app_` creates `app_*` and writes `app_*` — `test_the_tables_it_creates_are_the_tables_it_is_allowed_to_write`, parameterised over three prefixes |
| A worker whose lease expired cannot complete its stage | `reducer._lease_check` compares `now` against the persisted `lease_expires_at`, which `SqlAlchemyStageRepository.apply` writes | `409` `lease_expired` — `test_an_expired_lease_is_refused_as_lease_expired_under_SQL` |
| A late write from a superseded worker cannot count | the fence: `_lease` mints a strictly higher one per lease, and `_lease_check` refuses a command carrying an older one | `409` `stale_fence` — `test_a_stale_fence_on_completion_is_409` |
| A command naming a workflow this deployment does not run is refused | `reduce()` compares `command.workflow_digest` and `snapshot.workflow_digest` against `workflow.digest` | `422` `workflow_digest_mismatch` — `test_a_workflow_this_service_does_not_run_is_a_digest_mismatch` |
| A caller without the scope cannot start work | `principal.require("jobs:write")` at the top of `start_job` | `Unauthorized` at the engine — `test_a_caller_without_the_scope_cannot_start_a_job`; `401` over HTTP — `test_a_missing_scope_is_401` |

**The command request shape is CLOSED.** `CommandRequest` and
`JobStartRequest` both set `extra="forbid"`, so an unknown body field is
REFUSED with a 422 naming it rather than silently discarded. This is not
tidiness: a client that misspells `state_version` would otherwise have its
command applied at a version it never read — the concurrency guarantee in the
first row above, removed by a typo, at exit 200.

## Failure vocabulary

The distinctions are the product. Collapsing any of them destroys the
information the caller needs in order to decide what to do next.

| Code | Meaning | Safe to retry? |
| --- | --- | --- |
| `200` / `201` | applied. The stage run returned is the new state | n/a |
| `404` | this stage run is not visible to you. Covers "no such id" AND "another org's" | no — the two are deliberately indistinguishable |
| `409` | a refusal: the ledger was not in the state your command assumed. `detail` is the class | yes — after RE-READING. Retrying the same body will be refused identically |
| `422` | either a refusal that retrying unchanged will not fix, or a malformed body. See the note below | only after changing the request |
| `401` | not authenticated, or missing the scope | after authenticating |
| `5xx` | THIS SERVICE failed and decided nothing. The command may or may not have applied | yes — and until it succeeds, the caller does not know the state, which is not the same as a refusal |

**`422` carries two different `detail` shapes, and a caller must handle both.**
A refused command produces this service's own envelope, whose `detail` is a
string (the refusal class, or the message listing the valid command kinds). A
malformed body produces FastAPI's envelope, whose `detail` is a **list**. A
caller that parses a 422 `detail` as a string breaks on the first mistyped
field. They are published as two shapes on purpose
(`stage_run.v1.json#/properties/error` and `.../validation_error`).

A `403` is deliberately absent from these routes. `PolicyRefused` maps to 403 in
`app_factory._STATUS_BY_ERROR`, but no route here raises it: answering 403 for
another tenant's stage run would confirm the id exists, and a stage run id is
derivable from a dispatch key, so an attacker who can tell present from absent
can enumerate another tenant's jobs without ever reading one.

## Versioning

`schema/stage_run.v1.json` and `schema/command.v1.json` are published. A
published version is never edited in place; a change that removes a field or
narrows a type is `v2`. `tests/contract/test_workflow_schema_v1.py` validates
real responses against both files and asserts the published `CommandKind`,
`RefusalClass` and `StageStatus` enums are exactly the engine's own, so a
vocabulary the package adds surfaces as a red test rather than as an
undocumented status.

## What this blackbox does NOT own or promise

- **Policy.** This service does not decide whether a job MAY run.
  A separate policy service is the PDP; this service enforces what it is told and
  owns no policy engine of its own.
- **Doing the work.** The ledger records that a stage was leased, completed or
  failed. It does not execute a stage, call a model, or run an agent. A worker
  leases, does the work elsewhere, and comes back with a command.
- **Scheduling or retry timing.** There is no scheduler here. `expire` is a
  command a caller submits once the deadline has passed, not a background sweep
  this service runs.
- **The workflow itself.** The stages and their order are DATA, loaded from
  `WORKFLOW_DEFINITION_PATH`
  (`docs/decisions/0002-the-workflow-is-data-not-code.md`). This service ships
  no built-in workflow and refuses to start without one.
- **Client domain data.** The ledger holds ids, digests, statuses and fences.
  Payloads are digests, not contents.

## What is NOT proven

- **No deployed environment.** Nothing in this repository has been applied to an
  AWS account, and `iac/` was not exercised in the proof run — see
  [`proof.md`](proof.md).
- **No MySQL arm.** Every ledger assertion is against SQLite. The
  compare-and-set is one conditional `UPDATE` and both dialects execute it the
  same way, but "the ledger works on MySQL 8" is an untested claim here.
- **No concurrency under real parallelism.** The racing-caller arms are two
  sequential calls at the same `state_version`, which is what the conditional
  `UPDATE` actually arbitrates. No test runs two threads or two processes.
- **No e2e against a running container.** The sibling services have an `-e2e`
  repository that imports nothing from the service; this one does not yet.
