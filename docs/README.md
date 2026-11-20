# foundry-workflow docs

- `architecture.md` — the shape and why, including what was declined.
- `runbook.md` — how to run it locally, how to deploy it, how to roll it back.
- `decisions/` — one file per decision that cost something to make.
- [Multi-tenant workflow execution](multi-tenant-execution.md) — scheduling, migration and rollout plan.
- [Tenant identity and isolation](tenant-security.md) — credentials, transport and security release gates.
- [Mutual TLS for service APIs](service-mtls.md) — reusable Terraform profile, client onboarding and verification.
- [Multi-tenant workflow handover](multi-tenant-handover.md) — original scope, verification evidence, and closeout sequence.
