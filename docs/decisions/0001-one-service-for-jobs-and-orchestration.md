# 0001 — One service for jobs AND orchestration

**Status:** accepted, 2026-09-05

## The question

The estate has two apparent needs. It needs a durable JOB queue: submit work,
lease it, retry it, know it ran exactly once. It needs multi-agent
ORCHESTRATION: a run moves through plan, execute and review, several agents may
touch it, and none of them may act on state another has already superseded.

Those read as two products. Building them as two services was a live option, and
the owner's instruction was "you decide".

## The decision

One service, one ledger.

## Why

They are the same durable state machine with different callers.

Strip both descriptions to what the storage layer must guarantee and the same
four properties come out. A unit of work is claimed by exactly one worker at a
time — a lease. A worker whose claim was superseded must not be able to write
anyway — a fence. A caller may only advance state it has actually read — a
compare-and-set on a version. Submitting the same work twice must produce one
unit, not two — idempotency by a dispatch key.

A job queue needs all four. An orchestrator needs all four. There is no fifth
property that only one of them wants.

That core already existed, tested, in the upstream `shepherd-workflow` package:
thirteen source modules and 95 tests, with a prefix-parameterised SQL schema and
a write-guard. Splitting the service would have meant either two deployments of
that one package — two schemas, two sets of tables, two answers to "which ledger
holds this run" — or forking it, which is worse: two implementations of one
state machine where only one carries the package's suite.

## What it costs, stated plainly

**The deploy cadence is shared.** A change wanted by the job queue redeploys the
orchestrator too. That is a real cost and it is the one condition that would
reverse this decision: if the two callers ever need to deploy on genuinely
different schedules — a regulated release process on one side, continuous
delivery on the other — the shared cadence stops being an inconvenience and
becomes the reason to split.

**One blast radius.** A defect in the reducer affects both callers at once.
Mitigated by the fact that a split would have shared the same package anyway, so
the defect would have shipped to both regardless; only the ROLLOUT would have
been staggered.

**One tenancy boundary to get right.** Both callers' rows live in the same five
tables, separated by `org_id` and by nothing else. The engine answers `None`
rather than 403 for another org's stage run, and the contract's tenancy row is
the guarantee that keeps that honest.

## What was NOT decided here

That the two callers share a `workflow`. They do not. The workflow is data
(`0002-the-workflow-is-data-not-code.md`), so a job queue and an orchestrator
running against the same deployment run different definitions and their commands
are refused against each other's stage runs with `workflow_digest_mismatch`.
Sharing a service is not sharing a state graph.

## Reversal

Splitting later is a data migration, not a refactor: the tables would have to be
partitioned by workflow and one side re-pointed at a new prefix. That is
expensive but bounded, and `WORKFLOW_TABLE_PREFIX` is the seam it would use —
which is part of why the prefix is a parameter reaching both the schema builder
and the write-guard rather than a constant.
