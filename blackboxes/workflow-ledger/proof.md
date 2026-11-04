# Blackbox: workflow ledger — proof

Every command below was run against this repository at the commit that added
this file, on 2026-09-05, and its output is copied from the run described. A
claim in [`contract.md`](contract.md) that could not be demonstrated by a
command is named in "What is NOT proven here" instead of being asserted.

For publication, names that belonged to the original private estate (sibling
repositories, issue-tracker IDs and one host application's table prefix) were
replaced with neutral ones. The commands, counts and outcomes are unchanged; a
re-run today prints `app_` where the original run printed that host's prefix.

The interpreter was the upstream monorepo's virtualenv (Python 3.12.13, `ruff 0.16.6`).
Neither `shepherd_contracts` nor `shepherd_workflow_service` is pip-installed
into it — `PYTHONPATH=.:src:vendor` stands in for that, exactly as the
Dockerfile's own comment describes. `.` is on the path because the tests import
`tests.conftest`.

Every exit status below was written to its own file by its own command
(`cmd > out 2>&1` and then, on a SEPARATE line, `echo "EXIT=$?" > out.exit`) and
read back with `cat`, because a trailing `; echo EXIT=$?` on the same line
reports the echo's status, not the command's.

## 1. The service's own suite

```
$ PYTHONPATH=.:src:vendor PYTHONDONTWRITEBYTECODE=1 python -m pytest tests -q
161 passed, 2 warnings in 2.63s                         exit 0

$ PYTHONPATH=.:src:vendor PYTHONDONTWRITEBYTECODE=1 python -m pytest tests --collect-only -q
161 tests collected in 0.35s                            exit 0

$ ruff check src tests
All checks passed!                                      exit 0
```

Void-checked: 161 collected == 161 passed, exactly, with **no skips**. That last
part is a change from this slice. The suite was `171 passed, 8 skipped` before
the template's widget domain was removed; the 8 skips were its MySQL repository
contract arm, and they left with the file. A green suite containing a skip that
nobody re-reads is how a permanent silent skip survives, so a suite that now
skips nothing is worth stating rather than assuming.

## 2. The guarantees, end to end, against the real process

Not a test client. This is `python -m shepherd_workflow_service.main` — the exact
command `docker/entrypoint.sh` execs — bound to port 18771 with
`SERVICE_ENV=local`, `WORKFLOW_TABLE_PREFIX=wf_`, a temp SQLite file, and
`WORKFLOW_DEFINITION_PATH` pointing at this repository's own
`contracts/workflow.v1.json`. Driven with `curl`. Status code in brackets.

```
GET /healthz                                                             [200]
  {"status":"up"}

GET /readyz                                                              [200]
  {"ready":true}

POST /v1/jobs   {"job_id":"job_1","input_digest":"dig_1"}                 [201]
  {"stage_run_id":"srun_e6d6ba410e44dc29a127f18bf53c70fc","job_id":"job_1",
   "workflow":"shepherd.agent_run.v1","stage":"plan","status":"pending",
   "state_version":0,"fence":null,"attempts":1}

POST /v1/jobs   the SAME job over the SAME input                          [201]
  {"stage_run_id":"srun_e6d6ba410e44dc29a127f18bf53c70fc", ... identical ... }

POST .../commands   {"kind":"lease","state_version":0}                    [200]
  { ... "status":"leased","state_version":1,"fence":1,"attempts":2}

POST .../commands   the SAME lease again at state_version 0               [409]
  {"detail":"state_version_conflict"}

POST .../commands   complete with fence 8 (granted fence was 1)           [409]
  {"detail":"stale_fence"}

POST .../commands   {"kind":"teleport","state_version":1}                 [422]
  {"detail":"'teleport' is not a command; expected one of
             ['cancel', 'complete', 'expire', 'fail', 'lease', 'start']"}

POST .../commands   {"kind":"lease","state_version":1,"state_versoin":9}  [422]
  {"detail":[{"type":"extra_forbidden","loc":["body","state_versoin"],
              "msg":"Extra inputs are not permitted","input":9}]}

GET /v1/stage-runs/srun_e6d6...   as org_2                               [404]
  {"detail":"srun_e6d6ba410e44dc29a127f18bf53c70fc"}

GET /v1/stage-runs/srun_e6d6...   with no auth headers                   [401]
  {"detail":"missing x-org-id or x-subject-id header"}

GET /v1/stage-runs/srun_does_not_exist                                   [404]
  {"detail":"srun_does_not_exist"}

POST .../commands   complete with fence 1 (the one granted)              [200]
  { ... "status":"succeeded","state_version":2,"fence":1,"attempts":2}
```

Then, from the SQLite file the process wrote:

```
physical tables created (WORKFLOW_TABLE_PREFIX=wf_):
  wf_attempts wf_events wf_jobs wf_outbox wf_stage_runs
```

Four things in that transcript are the contract, not decoration.

**The two `POST /v1/jobs` calls return the same `stage_run_id` at
`state_version` 0.** The id is derived from the dispatch key, so the second call
did not create a second stage run — the database resolved it, not a
check-then-insert.

**The two refusals are 409s carrying the class verbatim**, and they are
different classes for different reasons: `state_version_conflict` says the
ledger moved, `stale_fence` says this caller's lease was superseded. An operator
triaging one needs to know which; a shared "conflict" message would not tell
them.

**Both 422s are real and their `detail` shapes differ.** The unknown command
kind carries a string; the misspelled field carries FastAPI's list. That is
exactly the wrinkle `contract.md`'s failure table warns about, observed rather
than predicted.

**Another org's stage run and a nonexistent one produce the identical answer** —
a 404 whose `detail` is the requested id. Nothing in the response distinguishes
"there is no such stage run" from "there is one and it is not yours".

One number in that transcript corrected a claim in the schema while this was
being written: `attempts` reads **1** on a freshly started stage run and **2**
after one lease, so it counts attempt ROWS (one at start, one per lease), not
leases. The schema said "one per lease granted", which is the plausible reading
and is wrong by one. It now says what was measured.

## 3. The two ledger properties, mutation-proved

Green arms prove nothing on their own. Both mutations below were applied to the
SOURCE, the suite re-run, and the file restored FROM A COPY TAKEN BEFORE THE
MUTATION — not `git checkout`, which would have discarded uncommitted work. The
baseline and both restore arms are printed, because an all-red table is
indistinguishable from a broken harness without them.

```
SHA_ENGINE_BEFORE  = 5390f2772a9db5027d8089b063c4289a4b27bb009b1030f340a8618d40cca963
SHA_COMMAND_BEFORE = f7e9370d51aecf345204a3953a7e5ab898717b2b682ff7f0723d5a1f63e96662

L0_BASELINE                          exit=0   14 passed
L1_prefix_literal_wf_                exit=1   2 failed, 12 passed
L1_prefix_literal_wf_ RESTORE        exit=0   14 passed        restore byte-identical: True
L2_version_from_snapshot             exit=1   1 failed, 13 passed
L2_version_from_snapshot RESTORE     exit=0   14 passed        restore byte-identical: True
L3_BASELINE_AGAIN                    exit=0   14 passed

SHA_ENGINE_AFTER   = 5390f2772a9db5027d8089b063c4289a4b27bb009b1030f340a8618d40cca963
SHA_COMMAND_AFTER  = f7e9370d51aecf345204a3953a7e5ab898717b2b682ff7f0723d5a1f63e96662
```

Target file for both arms: `tests/integration/test_sql_workflow_engine.py`.

**L1, the prefix rule.** `self._owned = owned_tables(table_prefix)` in
`adapters/real/sql_workflow_engine.py` replaced with
`owned_tables("wf_")` — a literal written at the default, which is the exact
defect the prefix rule exists to prevent. The two failures:

```
FAILED test_the_tables_it_creates_are_the_tables_it_is_allowed_to_write[app_]
FAILED test_the_tables_it_creates_are_the_tables_it_is_allowed_to_write[shepherd_wf_]

E   TableOwnershipViolation: shepherd-workflow would write
    ['wf_attempts', 'wf_events', 'wf_jobs', 'wf_outbox', 'wf_stage_runs'],
    which the unit of work does not own (granted:
    ['app_attempts', 'app_events', 'app_jobs', 'app_outbox', 'app_stage_runs'])
```

**Read the `[wf_]` arm, because it stayed GREEN under this mutation.** That is
why the test is parameterised over three prefixes and why a single-prefix
assertion would have been worthless: at the default the literal and the
parameter agree, so the defect is invisible from the default alone. A host on
`app_` creates `app_jobs` and is then refused it by a guard comparing against
`wf_` — a service refused the tables it had just created, reported as an
ownership violation against itself.

**L2, the concurrency rule.** `expected_state_version=state_version` in
`models/workflow_command.py` replaced with
`expected_state_version=snapshot.state_version` — the version the ledger
currently holds rather than the one the caller said it read, which is the shape
that makes every compare-and-set trivially true:

```
FAILED test_two_callers_at_the_SAME_state_version_do_not_both_win
```

One arm, and it is the right one: with that mutation the conditional `UPDATE`
compares the row against itself, so both racing callers win and the second
silently overwrites the first. Nothing else in the file notices, which is what
makes that single test load-bearing.

Both mutated files are byte-identical before and after (same SHA-256).

## 4. The published schemas are enforced, not described

`tests/contract/test_workflow_schema_v1.py` validates real responses — a started
job, an accepted command, a read-back, a 409 refusal, a 404, a 401, both 422
shapes — against `schema/stage_run.v1.json` and `schema/command.v1.json`, and
asserts the published `CommandKind`, `RefusalClass` and `StageStatus` enums are
exactly the engine's own.

```
$ PYTHONPATH=.:src:vendor python -m pytest tests/contract/test_workflow_schema_v1.py -q
14 passed, 2 warnings in 0.39s                          exit 0
```

Fourteen green arms prove nothing on their own — a validator that accepted
anything would pass them all — so each schema was weakened on purpose and the
suite re-run. Note that these mutations leave the FILE in place and remove one
CONSTRAINT: deleting a schema file would only prove the gate checks a file
exists, while removing a constraint asks whether it compares what it found
against what the schema declares.

```
SHA_STAGE_RUN_BEFORE = a47e9bb50dfa8607162facbd3c20331749aa93478d85251f7c014d0856b53ff0
SHA_COMMAND_BEFORE   = ac535696c025aae1c471b1fe6bd3160e792740e017d3d824a569e00f8a213058

S0_BASELINE                            exit=0   14 passed
S1_request_accepts_unknown_field       exit=1   1 failed, 13 passed
S1_..._RESTORE                         exit=0   14 passed    restore byte-identical: True
S2_attempts_no_longer_required         exit=1   1 failed, 13 passed
S2_..._RESTORE                         exit=0   14 passed    restore byte-identical: True
S3_a_refusal_class_dropped             exit=1   1 failed, 13 passed
S3_..._RESTORE                         exit=0   14 passed    restore byte-identical: True
S4_BASELINE_AGAIN                      exit=0   14 passed

SHA_STAGE_RUN_AFTER  = a47e9bb50dfa8607162facbd3c20331749aa93478d85251f7c014d0856b53ff0
SHA_COMMAND_AFTER    = ac535696c025aae1c471b1fe6bd3160e792740e017d3d824a569e00f8a213058
```

S1 removes `additionalProperties: false` from the command request; S2 drops
`attempts` from the stage run's `required` list; S3 deletes one member from the
published `RefusalClass` enum. The third is the one that keeps the published
vocabulary honest as the vendored package changes: a class added there and not
published here goes red rather than becoming an undocumented status code.

**What arm S3 does NOT prove**, stated because it is easy to over-read: it
proves the enum is compared against `RefusalClass`, not that every published
class is reachable through a route. FOUR of the eleven — `terminal_state`,
`cancelled`, `feature_disabled` and `dispatch_key_mismatch` — appear in no
assertion anywhere under `tests/`, measured as a literal-count per class with
`state_version_conflict` (4 hits) as the positive control that the count works
at all. They are published because they are part of the engine's vocabulary and
a caller may meet them; they are not published as demonstrated.

## 5. `LEASE_EXPIRED` is reachable again, and this repository now says so

A tracked defect recorded that `SqlAlchemyStageRepository.apply` never
persisted `lease_expires_at`, so under SQL the column stayed NULL however far
the clock moved and `RefusalClass.LEASE_EXPIRED` could not be produced, while
the in-memory reference produced it happily. The fix landed in the upstream
`shepherd-workflow` package and was re-vendored here in
`f0d9903`; the vendored tree is byte-identical to the package
(`/usr/bin/diff -rq -x __pycache__` exit 0).

Two arms in `tests/integration/test_sql_workflow_engine.py` hold it HERE,
because nothing else would have gone red: `tests/contract/test_engine_conformance.py`
compares the two engines at a single frozen instant, and this property is only
observable across two.

The second arm is the control, and it is the reason the first one's numbers are
what they are. `reduce()` checks `now >= deadline_at` BEFORE it checks the lease,
so a job deadline shorter than the lease duration would make the first arm green
while proving nothing about `lease_expires_at`. Driving the identical script with
a three-minute job deadline instead of an hour changes the refusal to
`deadline_passed`, so the two are demonstrably distinguishable in this wiring
rather than merely different names in an enum.

## What is NOT proven here

- **No deployed environment.** Nothing in this repository has been applied to an
  AWS account. `iac/` was not exercised in this run — no `terraform validate`,
  no `serverless print`, and `scripts/check_service_iac.sh` was not invoked.
- **No container.** Section 2 drives the real process, not the real IMAGE.
  `Dockerfile`, `docker/` and `docker-compose.yml` were owned by a concurrent
  slice and were deliberately not touched or run here, so
  "the image serves these routes" is an untested claim in this document.
- **No CI signal.** GitHub Actions cannot execute on this account
  (billing). Every number above is from a local run.
- **No MySQL.** Every ledger assertion is against SQLite. The compare-and-set is
  one conditional `UPDATE` that both dialects execute the same way, but "the
  ledger works on MySQL 8" has not been demonstrated. There is no MySQL arm to
  skip, either — the file that used to carry one belonged to the deleted
  example domain.
- **No real parallelism.** The racing-caller arms are two SEQUENTIAL calls at
  the same `state_version`, which is precisely what the conditional `UPDATE`
  arbitrates. No test runs two threads or two processes, so contention under
  load is unmeasured.
- **No e2e repository.** The sibling services each have an `-e2e` repository
  that imports nothing from the service, so a green service suite and a green
  e2e run are two independent claims. This service has no such repository yet,
  so both claims here come from one tree.
- **No drift gate over `vendor/`.** The byte-identical check in section 5 was
  run by hand. No gate compared the vendored copy
  against the package it was copied from, so nothing would notice the day they
  diverge.

---

## ADDENDUM — 2026-09-05: `wf_outbox` is written, and nothing drains it

This is an addendum rather than an edit. The record above is what was measured on
the day it was taken and stays as it stands; this section records something learnt
afterwards about one line in it.

Section 2 of this proof lists the physical tables the live run created:

```
wf_attempts wf_events wf_jobs wf_outbox wf_stage_runs
```

`wf_outbox` is real. `SqlAlchemyOutbox` is built at
`adapters/real/sql_workflow_engine.py:226` and written by the vendored
`runner.py`'s `self._outbox.enqueue(...)` inside the engine's own transaction, so
rows land there transactionally with the stage-run write, which is the whole point
of the shape.

**Nothing in this service moves them out again.** `OutboxDispatcher` exists in the
vendored package (`vendor/shepherd_workflow/dispatcher.py`) and is constructed
nowhere under `src/` or `tests/` — zero hits for `OutboxDispatcher` in either tree,
against a positive control of two `SqlAlchemyOutbox` hits under `src/`, so the zero
is an absence rather than an ungreppable query. Rows accumulate with no consumer.

**Why this was not visible before, and why it is worth writing down.** Until today
`iac/serverless.yml` declared a scheduled EventBridge function named `drainOutbox`.
Reading this proof next to that config, the obvious inference was that the table
listed here had a drainer deployed against it. It did not, twice over: that
function's entire body imported a symbol and exited, and the module it imported
belongs to the template-inherited `Publisher`/`OutboxEventBus` pair, which is a
DIFFERENT outbox from the ledger's and is always empty because nothing in this
service publishes through it. The function has been deleted, which removes the false assurance and removes no durability,
since it never drained anything.

What this proof can now be read as saying about the ledger outbox, exactly: rows
are written transactionally, and their delivery is unproven and at present
unimplemented in this service. That is a gap in the mechanism, not in the record.

---

## ADDENDUM — 2026-09-05 (second): the outbox is drained, and one line of the first addendum was wrong

An addendum rather than an edit, including where it corrects the addendum above it.
The earlier record stays as it stands; this section records what draining the table
turned up.

### The gap is closed

`src/shepherd_workflow_service/ledger_drain_main.py` is a real process --
`python -m shepherd_workflow_service.ledger_drain_main` -- that resolves this
deployment's configuration, builds the container, and calls
`SqlWorkflowEngine.drain_pending`, which drives the vendored `OutboxDispatcher` over
the package's own `pending()` / `mark_sent()` / `mark_ambiguous()`. Nothing in
`vendor/` was changed, so the vendored copy's byte-identity with the upstream
package is untouched.

The decision between "wire it" and "declare it write-only" was not close. The package
means the rows to move: `OutboxSink`'s own docstring says "a separate dispatcher then
moves rows to the broker at-least-once", the table carries a nullable `sent_at`, an
`ambiguous` flag and an `ix_<prefix>outbox_pending` index on `(sent_at, created_at)`
that exists only to find undelivered rows, and three reader/marker methods sat with no
caller in this service.

**The cost was lost delivery rather than table growth, and the measurement that
settles it is about a DIFFERENT table.** The package declares `events` as its audit
record. In this service nothing writes it: `git grep '_events\|\["events"\]' -- src`
returns nothing, against a positive control of two `["attempts"]` insert hits in the
same tree. `wf_events` is created and stays empty, so `wf_outbox` held the ONLY
durable record of a stage transition. Undelivered, that includes `stage.dispatched`,
whose payload carries `successor_dispatch_key` -- the signal that the next stage is
ready. Unbounded growth was a symptom of the same fact.

### The correction: the deleted Lambda never imported anything

The addendum above, the service's status notes and `iac/serverless.yml` all recorded the deleted
`drainOutbox` function as one whose "entire body imported a symbol and exited". That
diagnosis is wrong, and the true one is worse.

`image.command` sets the container's CMD. This image's `ENTRYPOINT` is
`docker/entrypoint.sh`, and that script did not name `"$@"` anywhere -- confirmed by
grep, exit 1, zero matches. So the CMD arrived as arguments and was discarded.
Measured 2026-09-05 by running the script against a stub interpreter, three cells:

```
no arguments                                          -> STUB_PYTHON_ARGS=-m shepherd_workflow_service.main
[python, -m, shepherd_workflow_service.ledger_drain_main] -> STUB_PYTHON_ARGS=-m shepherd_workflow_service.main
[/opt/venv/bin/python, -c, "from ...outbox_drain import handle_scheduled_drain"]
                                                      -> STUB_PYTHON_ARGS=-m shepherd_workflow_service.main
```

What EventBridge invoked 1440 times a day was the HTTP API server.

Two consequences worth more than the original finding. First, the IaC gate's arm 6 --
added specifically to refuse an import-only command -- was **checking a string the
runtime threw away**. Its classifier is correct and its inline controls hold; the
input simply had no effect on anything. No control inside a gate can detect that its
subject is inert, because the gate cannot see the layer that consumes its subject.
`docker/entrypoint.sh` now execs an explicit command when it is given one, which is
what makes that arm load-bearing rather than decorative;
`tests/shape/test_entrypoint.py` pins both directions, including the negative control
that an empty argument list still reaches `main`.

Second, no scheduled function was added to `iac/serverless.yml`. This is an
image-package-type Lambda whose one process is the AWS Lambda Web Adapter forwarding
each invocation to a listening HTTP server; a CMD that runs a one-shot script and
exits leaves the adapter nothing to forward to. A `drainLedgerOutbox` function there
would be a second function whose name asserts an action it does not perform. The
honest surface is a scheduled ECS task against `iac/terraform/`, and it is filed
rather than implied.

### The drain drains -- proof, with its control

`tests/integration/test_ledger_drain_entrypoint.py` runs the entrypoint as a REAL
subprocess against a real HTTP queue endpoint, so `ServiceConfig.from_environment()`,
`build_container()`, `boto3` signing, the SendMessage over the wire and the ledger
marking are all exercised as deployed rather than as imports.

```
seed one accepted command  -> 1 pending row in wf_outbox
run the entrypoint process -> exit 0, stdout {"published": 1, "ambiguous": 0, "failed": 0}
                              the queue received it, carrying the stable key
                              pending rows in wf_outbox: 0
CONTROL: same ledger, entrypoint NOT run -> pending rows in wf_outbox: 1
CONTROL: queue configured but unreachable -> exit 1, failed=1, pending rows: 1
CONTROL: SQS_QUEUE_URL unset               -> exit non-zero naming it, pending rows: 1
```

The last two controls carry the design's sharpest edge. A drain with no durable
downstream must refuse: falling back to `InProcessEventBus`, which has no subscribers
in any deployed process, would publish nothing, mark every row sent, and report a
clean run -- emptying `wf_outbox` and destroying the events, which is strictly worse
than the undrained table this addendum was opened about. `_ledger_broker_factory`
therefore takes the INJECTED publisher rather than the container's resolved one, whose
own fallback is exactly that in-process bus.

### Measurements

Each exit code was written to its own file by its own command and read back.

```
pytest tests                 183 passed, 0 skipped   exit 0
pytest tests --collect-only  183 collected           exit 0   void check: 183 == 183 + 0
ruff check src tests         All checks passed!      exit 0
bash scripts/check_service_iac.sh
        PASS: all 8 arm(s) executed and green (tofu + serverless@3)     exit 0
```

The full mutation table, with the baseline and every restore arm, was kept in the
service's private status notes and is not part of this repository.
