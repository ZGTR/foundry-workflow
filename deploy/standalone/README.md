# Independent Workflow deployment

This Compose file describes a local container deployment. The accompanying probe verifies the real image with signed authentication, retained state and backup/restore. The example deployment serves a printable-generation pipeline owned by a separate application; that application's production wrapper supplies its own execution, identity and database contracts. Keep this repository and its service deployment independent of any one application.

Build the image from this repository's reviewed revision. Resolve its immutable `repository@sha256:...` digest and record it separately from the calling application's revision. The `shepherd-workflow-printables-v1` Compose project belongs to this repository; the printables release must not recreate or remove its database. No Restate process or application-embedded copy of the reducer participates.

The operator supplies an explicit, hash-verified workflow definition from the calling application's release. A definition change creates a new deployment identity, endpoint and table prefix; existing jobs retain their old definition until drained. The source-printable definition currently owns source, normalize, compile, render and verify. Its source stage has one automatic attempt; output stages use their declared bounded retry policy.

Prepare `WORKFLOW_DATA_DIR` on the encrypted retained volume, owned by the image's UID/GID 65532 and mode 0700. Confirm that the volume is mounted before starting Compose. Missing bind paths fail instead of creating a fresh empty database silently. Preserve the directory when changing images. The application listens through a loopback-only port; callers on another host need a separately reviewed authenticated TLS transport.

Production requires a trusted asymmetric workload issuer, an audience of `shepherd-workflow-printables` and a read-only public JWKS file. Keep signing keys outside these containers. The small `jwks` service serves only this public file on the Compose network and host loopback. The pinned PyJWT version deliberately rejects `file:` URIs; use its supported HTTP resolver rather than weakening that restriction. The release gate must demonstrate valid signed requests, refusal of forged/expired/wrong-audience tokens and refusal of development identity headers. Image runtime compatibility remains an explicit test to run.

When rotating keys, publish old and new public keys together, recreate the JWKS container to remount the immutable file, allow its verifier cache interval to elapse, then issue tokens with the new key. Retire the old public key only after the last old token expires. Never mount a directory containing private keys into the static server.

Required release evidence:

1. Validate the Compose model with the actual immutable image and exact mount paths.
2. Launch the real image with a fresh retained database; prove auth positives and negatives.
3. Persist a stage, restart the container and resume with unchanged job/stage identity.
4. Back up SQLite through its online backup API, retaining a consistent database rather than copying a live WAL file. Restore to a separate directory, verify database integrity and replay an existing completed stage without duplicating its completion. Whole-job execution remains an application integration check.
5. Repeat transient failure, retry delay, stale-fence, concurrent claim and terminal replay probes against the deployed endpoint.
6. Record image digest, source revision, definition hash, database schema version and sanitized proof in this repository. Never include tokens, signing keys or customer source content in service proof.

Rollback selects the previous immutable image and compatible definition against the retained database. Schema migrations require their own forward/backward compatibility review. A healthy process is not proof of durable recovery or printable content quality.

The image builds from the universal Python 3.12 lock on Linux ARM64 and x86-64; an earlier platform-specific lock omitted Linux greenlet. An ARM64 build does not prove a native x86-64 build.

Run the isolated container probe with the built image and the application's exact workflow definition:

```sh
.venv/bin/python deploy/standalone/verify_container.py \
  --image workflow:local \
  --definition /absolute/path/to/workflow-definition.json \
  --receipt "$HOME/workflow-proofs/run-1/durability.json"
```

The probe resolves the image ID before starting, creates uniquely named Docker resources, keeps signing keys in memory, and removes only its own containers, volumes and network afterward. It copies public JWKS and workflow configuration into an isolated Docker volume, so the daemon does not need access to host bind paths. It rereads the published port after restart because Docker can assign a different ephemeral port. The receipt records the image, probe and definition hashes. Existing receipts cannot be overwritten.

These are local container checks. A production deployment, its database integration and real application jobs need their own release evidence.

## Local MySQL ledger verification

`verify_mysql.py` runs the existing SQL engine against a fresh isolated MySQL 8.0.46 container. It uses an image already present locally, publishes only a loopback port, generates disposable credentials and removes its own container and volume. Existing databases are never selected. Supply `PYTHONPATH=.:src:vendor` when invoking it with the same `--image`, `--definition` and `--receipt` arguments; the image argument names the local MySQL image.

The MySQL receipt records ten checks: idempotent start, tenant isolation, one winning concurrent claim, stale-fence refusal, retained lease after database restart, duplicate completion refusal, persisted retry delay with a new fence, the single-attempt source limit, all five stages reaching `review_wait`, and identical retained stage state after another restart. These are ledger commands with labelled test artifacts, not teaching-content generation or production database proof.

Both probes now publish success only after Docker confirms their owned resources are absent. They bind the probe bytes before execution and refuse a receipt if those bytes change; the MySQL probe also binds the imported runtime source tree. Six negative unit controls cover false cleanup success, unavailable Docker, an already absent resource, unrelated resource names, changed source and receipt collisions. Receipts are written where `--receipt` points and are not committed to this repository.

## Local MySQL tenant admission verification

Run the tenant admission probe with the development dependencies and a locally available MySQL 8.0.46 image.

```sh
PYTHONPATH=.:src:vendor .venv/bin/python deploy/standalone/verify_mysql_tenant_admission.py \
  --image mysql:8.0.46 \
  --receipt /tmp/workflow-mysql-tenant-admission.json
```

The probe uses the checked-in `contracts/examples/catalog.v2.json` and a fresh, loopback-only database. It sends signed certificate-bound test credentials through the workload authenticator and exercises tenant-scoped submissions, reads and completions against the SQL ledger. Concurrent workers exercise pool, tenant and backlog caps. The probe injects a failure before lease commit, restarts MySQL mid-visit, checks revocation and verifies that migration changes ownership comparisons from collation aliases to byte-exact matches. The receipt binds the catalog, probe and runtime sources and records Docker cleanup.

The test simulates a verified gateway peer in-process. Validate the deployed gateway, certificate revocation, issuer and external transports in staging.
