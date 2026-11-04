# Security policy

## Reporting a vulnerability

Report suspected vulnerabilities privately by email to the maintainer, using the
contact address on the maintainer's GitHub profile (https://github.com/ZGTR).
Include reproduction steps, the affected commit and the impact. Do not open a
public issue, and do not include credentials or personal data. This project does
not promise a response-time SLA. Fixes land on `main`; there is no long-term
support branch.

## Threat model

| Asset | Threat | Control |
|---|---|---|
| Caller identity | a caller asserts an org, subject or scope it was not issued | outside `SERVICE_ENV=local`/`test`/`dev` the service refuses to start on unverified header auth (an unrecognised environment name counts as production); `adapters/real/jwt_principal_auth.py` takes the principal from signed claims only, with an algorithm allow-list that excludes `none`, `kid`-selected keys, and required issuer and audience |
| Tenant isolation | one org reads or enumerates another org's stage runs | every row carries `org_id`; another org's stage run answers 404, not 403, so its existence is not disclosed |
| Ledger integrity | two callers advance the same stage run, or a refused command writes partial state | a version-conditional `UPDATE` admits one winner per `state_version`; a refusal leaves its transaction by raising, so it rolls back |
| Table ownership | the service writes tables outside its own set in a shared database | `owned_tables(prefix)` and `build_tables(prefix)` derive from one `WORKFLOW_TABLE_PREFIX`, and `assert_writes_within` refuses any other table |
| Database credentials | leaked through logs, the environment or the image | MySQL reads the secret's name (`DB_PASSWORD_SECRET_ID`) and fetches the password from AWS Secrets Manager at startup; no secret is baked into the image |
| Supply chain | a dependency or base image changes under a green build | base images pinned by digest; runtime dependencies installed with `pip install --require-hashes` from `requirements/service.lock`; the vendored workflow package pinned by hash in `vendor/workflow-provenance.json` |
| Container runtime | a compromised process rewrites application code | numeric non-root user (`65532`) and a read-only application tree |

`SERVICE_ENV=dev` deliberately accepts the unverified development headers unless
`AUTH_JWKS_URI` and `AUTH_AUDIENCE` are set; do not expose a `dev` deployment to
untrusted callers.

Out of scope: denial of service at the network edge, the identity provider that
issues tokens, and the AWS account a deployment runs in.
