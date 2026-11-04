# 0002 — The workflow is data, not code

**Status:** accepted, 2026-09-05

## The decision

The stages a job moves through, and their order, are a JSON document loaded at
runtime from `WORKFLOW_DEFINITION_PATH`. The service ships no built-in workflow
and refuses to start without one.

```json
{"workflow_key": "shepherd.agent_run.v1",
 "stages": [{"key": "plan",    ...},
            {"key": "execute", ...},
            {"key": "review",  ...}]}
```

The obvious alternative — a module with a `STAGES = ["plan", "execute",
"review"]` constant, or a class per stage — was declined.

## Why

**A digest is the version number.** `WorkflowDefinitionV1` hashes to a digest,
every stage run stores the digest it was created under, and every command is
checked against it. Change the definition and the digest changes, so commands
against stage runs created under the old one are refused with
`workflow_digest_mismatch` rather than being silently reinterpreted. A code
constant has no digest. Changing it changes the meaning of rows already in the
database, with nothing to compare and nothing to refuse.

**Two deployments can run different workflows against one service.** This
follows directly from decision 0001: if a job queue and an orchestrator share a
service, the thing that must differ between them is the stage graph. As data
that is a file path. As code it is a branch on a caller-supplied string, inside
the reducer, which is exactly where this design refuses to put a branch.

**The reducer stays pure.** `reduce()` takes a workflow, a snapshot, a command
and a `now`, and returns a plan. It reads no module-level constant, so it is
testable at three literals and cannot behave differently in one deployment than
in another for reasons nobody can see in its arguments.

## Why there is no default

A built-in default workflow is the failure mode this decision is most concerned
about, and it is not obvious, so it is worth spelling out.

A service that invented a default would START successfully with
`WORKFLOW_DEFINITION_PATH` unset. It would accept `POST /v1/jobs` happily. Every
subsequent command would then be refused with `workflow_digest_mismatch` —
because the caller is working against the workflow it thinks is deployed and the
service is working against the one it invented.

That refusal reads as a caller bug. An operator would look at the client, at the
definition the client holds, at the digest arithmetic — everywhere except at a
missing environment variable, because the service started, and a service that
starts is a service that was configured.

So `build_container()` raises instead, at startup, naming the variable and
pointing at `contracts/workflow.v1.json` as an example. The service is not able
to run in the state that would produce the confusing symptom.

## What it costs

**A file has to be present in the image.** The Dockerfile must copy the
definition and the deployment must set the path — one more thing that can be
wrong, and it fails loudly rather than subtly, which is the trade.

**The definition is not validated against a schema on load.**
`load_workflow_definition` parses JSON and builds `WorkflowDefinitionV1`; a
structurally valid document describing a nonsensical workflow is accepted.
Unproven, not solved.

**Changing it under a running service is a breaking change to existing rows.**
That is the digest working as intended, but an operator who edits the file to
"just add a stage" will find every in-flight job refusing commands. The runbook
names `workflow_digest_mismatch` for this reason.
