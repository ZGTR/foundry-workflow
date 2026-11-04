# foundry-workflow — architecture

## What it is

A durable ledger for long-running work, served over HTTP. A caller starts a job,
leases a stage of it, does the work somewhere else, and comes back with a
command. The ledger's only job is to make sure that at most one caller's version
of what happened wins, and that everyone else is told, precisely, why they lost.

It serves both of this estate's callers — a durable job queue and multi-agent
orchestration — because they are the same state machine with different callers.
That was a decision with a real alternative;
`decisions/0001-one-service-for-jobs-and-orchestration.md` records the cost.

## Layers

```
controllers/    HTTP in, HTTP out. Parse, authenticate, call, serialise.
                Every handler under 12 lines, mechanically.
schemas/        Pydantic at the edge. `extra="forbid"` on every request model.
services/       The application service. Owns ONE thing: the translation from a
                refusal to an HTTP error. Holds no state machine.
ports/          Protocols this service needs from the world.
adapters/       fake/ and real/ implementations, one file per port name.
models/         Pure translation between the wire and the engine's command type.
config.py       The ONLY module that reads the environment.
container.py    The DI root. Explicit constructor wiring, no service locator.
```

`tests/shape/test_layering.py` enforces five of those sentences rather than
trusting them: no module outside `config.py` reads the environment, the domain
imports no adapter, every port has a fake, every handler is under the line
limit, and the container builds with no network. Each check has a test that
proves it goes RED on a deliberate violation — a check that has never failed has
not been tested.

## The package is VENDORED, not imported

The state machine lives in `shepherd_workflow`, a package that started in an
upstream monorepo. It is copied into `vendor/` and shipped inside this
repository.

That is a deliberate trade and it costs something. The upside is that this
service is a standalone deployable artifact: `git clone` plus `docker build`
produces a running image with no private index, no path dependency on a sibling
checkout, and no chance that a `pip install` resolves a different version than
the one a proof was taken against. The downside is drift — nothing today
compares `vendor/shepherd_workflow` against the package it came from, so the day
they diverge nobody is told. The check was run by hand
for this slice and the trees are byte-identical; "by hand" is exactly the
property that does not survive.

Vendoring is not forking. A defect found in the package — such as
`SqlAlchemyStageRepository.apply` not persisting `lease_expires_at` — is fixed
once, in the one copy. This repository now owns that copy: every change to
`vendor/shepherd_workflow` updates the hashes in `vendor/workflow-provenance.json`,
and `tests/contract/test_workflow_provenance.py` refuses a tree that does not
match them.

## The engine port faces the OPPOSITE way to the package's own ports

This is the piece worth reading slowly, because `shepherd_workflow` already has
a `ports/` module and it would be reasonable to assume this service just
implements those.

The package's ports — `StageRepository`, `OutboxSink`, `UnitOfWork` — are what
the package needs FROM its host. They point outward from the package, and this
service satisfies them with SQLAlchemy implementations.

`ports/workflow_engine.py` points the other way: it is what this service's HTTP
layer needs FROM the engine. Start a job, submit a command, read a stage run.
The controller depends on a contract this repository owns rather than on the
package's concrete `StageRunner`.

The practical consequence is the one that matters. The layering test already
forbids a controller importing an adapter; this keeps that true without the
controller having to know that a `UnitOfWork` exists, that commands carry
fences, or that the reducer is pure. The engine's vocabulary stays inside the
engine, and `StageRunView` exposes only what a caller must act on:
`state_version`, because a caller submitting a command has to say which version
it read, and `fence`, because a worker granted a lease must echo it.

Two implementations satisfy that port. `SqlWorkflowEngine` is what deploys.
`InMemoryWorkflowEngine` is the fake — and it is a fake only in the sense that
its storage is a dict. It drives the same `StageRunner`, the same pure
`reduce()`, and the package's own `InMemoryStageRepository`, whose compare-and-set
is the reference semantics. A hand-written stand-in returning canned outcomes
would agree with every test written against it and prove nothing about the
deployed service. `tests/contract/test_engine_conformance.py` runs one script
against both and compares the answers.

## The transaction boundary

`SqlWorkflowEngine._apply` opens a transaction, loads the snapshot, runs the
reducer, and applies the plan — all inside `connection.begin()`.

A refusal leaves that block **by raising** (`_Refused`), so the transaction rolls
back. That shape is doing real work: the outbox writes happen in the same
transaction, so "the command was refused and its events were published" is ruled
out structurally rather than by remembering to check. `_Refused` is not an error
travelling to the caller; it is a rollback signal, caught immediately and turned
back into an ordinary `CommandOutcome`.

Above that, `WorkflowService.submit` turns the refusal into an exception —
`StageRunConflict`, `StageRunNotFound` or `StageRunRejected` — and
`app_factory._STATUS_BY_ERROR` turns that into a status code, once, for the
whole service. No controller carries a `try`/`except`, which is why the handlers
stay four lines. The refusal is an ordinary answer inside the engine and becomes
a status code only at the HTTP edge, because an HTTP caller has no other channel
for "understood, and no".

An unrecognised refusal class falls through to 422, not 409. The package can add
a class; mapping the unknown one to a conflict would tell the caller to retry
something that may never succeed.

## The table prefix is ONE value

`WORKFLOW_TABLE_PREFIX` reaches `build_tables(prefix=...)`, which names the
physical tables, AND `owned_tables(...)`, which names the set the write-guard
permits. Both come from `SqlWorkflowEngine.__init__`'s single `table_prefix`
argument.

A literal `wf_` set on either side would be correct only at the default. This
estate has already paid for that exact defect: a host on `prefix="app_"` created
`app_jobs` and was then refused it by a guard comparing against a hardcoded
`wf_` set, so the service was denied the tables it had just created.
`tests/integration/test_sql_workflow_engine.py` parameterises over three
prefixes for that reason — the `wf_` arm stays green under the mutation, so a
single-prefix assertion would have proved nothing.

## What was declined

**A policy engine.** This service enforces; a separate policy service decides. A
second place that can say no is a second place that can disagree.

**A scheduler.** `expire` is a command a caller submits after the deadline
passes, not a background sweep. A sweep would need its own leader election,
its own failure mode, and its own answer to "why did this job move at 3am".

**A built-in workflow.** There is no default. A service that invented one would
accept jobs it can never advance, and every command would be refused as a digest
mismatch — an error message pointing at the caller for a defect in the
deployment. `build_container()` refuses to start without
`WORKFLOW_DEFINITION_PATH`, with the fix in the message.
See `decisions/0002-the-workflow-is-data-not-code.md`.

**A fall-through to SQLite.** An unrecognised `DB_BACKEND` is refused at
startup. Quietly handing a durable ledger a local file nobody backs up fails as
missing data, long after the typo. The eager refusal and the lazily-resolved
URL both call `_require_a_buildable_database`, so there is one authority rather
than two branch lists that agree until one of them learns a new backend.

**Its own migrations, for now.** `metadata.create_all(checkfirst=True)` on first
connect is a no-op against a schema a migration already created. It exists so a
local run and a fresh container need no provisioning step. A deployed MySQL
service is expected to run migrations; that path is not yet built here.

**The template's example domain.** The service template ships a worked
`widget` domain. It was deleted rather than kept as a reference, because example
code that survives into a real service is indistinguishable from product to
every reader after the first.
