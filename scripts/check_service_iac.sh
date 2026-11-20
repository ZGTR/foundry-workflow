#!/usr/bin/env bash
# check_service_iac.sh -- prove this service's IaC parses, resolves and
# validates, instead of asserting it in a comment.
#
# This is the service-repo descendant of the monorepo's
# `scripts/check_template_iac.sh`, which found a real defect on its first run:
# `serverless print --config <subdir>/serverless.yml` from a parent exits 1, so
# the template's deploy step could never have worked. That file is checked into
# a different repository, so this one is not a copy for tidiness -- the arms
# have to run where the IaC lives or they do not run at all. `new_service.sh`
# emits this file into every service it creates, with `SERVICE` below rewritten
# to the new name by the same pass that renames the Python module.
#
# A service with its own model-egress, network-egress or other scoped-grant
# requirement adds its own arm here (for example, an LLM gateway's arm that
# checks a resolved Bedrock IAM grant against an allow-list) and bumps
# EXPECTED_ARMS to match -- that arm is specific to what the gateway deploys,
# not to every service this template emits.
#
# NO CONDITIONAL ARMS. Every arm runs or this script refuses. A gate that skips
# when a binary is missing exits 0 having checked nothing, and that zero is
# byte-identical to a real pass. Missing tooling is a REFUSAL (exit 2) naming the
# install command.
#
# Usage:  bash scripts/check_service_iac.sh

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TF_DIR="$ROOT_DIR/iac/terraform"
SLS_DIR="$ROOT_DIR/iac"
WORKFLOW="$ROOT_DIR/.github/workflows/ci.yml"
SERVICE="foundry-workflow"

# EIGHT, not seven. Eight calls to `arm` reach the accumulator below, and this
# constant read 7 -- so the floor bound only seven of them and an arm could have
# been deleted with the verdict still claiming full coverage. A floor that does
# not equal the number of arms is the "category count is not a coverage claim"
# defect in miniature: the terminal line said "all 8 arm(s) executed" from RAN,
# while the thing that could REFUSE was comparing against a smaller number.
EXPECTED_ARMS=9
RAN=0
FAILED=0

# Every arm goes through here, so the count in the verdict cannot claim an arm
# that an abort skipped -- there is no `set -e`, and a red arm records itself and
# lets the rest run.
arm() {
  local name="$1"
  shift
  RAN=$((RAN + 1))
  if "$@"; then
    echo "[service-iac] ok: $name"
  else
    echo "[service-iac] FAIL: $name" >&2
    FAILED=$((FAILED + 1))
  fi
}

refuse() {
  echo "[service-iac] REFUSED: $1" >&2
  exit 2
}

# --- refusals: the inputs this gate cannot check without ----------------------

[ -d "$TF_DIR" ] || refuse "no terraform module at ${TF_DIR#"$ROOT_DIR"/}"
[ -f "$SLS_DIR/serverless.yml" ] || refuse "no serverless.yml at ${SLS_DIR#"$ROOT_DIR"/}"
[ -f "$WORKFLOW" ] || refuse "no workflow at ${WORKFLOW#"$ROOT_DIR"/}"

TF_BIN=""
for candidate in terraform tofu /opt/homebrew/bin/tofu; do
  if command -v "$candidate" >/dev/null 2>&1; then
    TF_BIN="$candidate"
    break
  fi
done
[ -n "$TF_BIN" ] || refuse "neither 'terraform' nor 'tofu' is on PATH; \`brew install opentofu\`"

command -v npx >/dev/null 2>&1 || refuse "'npx' is not on PATH; install Node to run 'serverless print'"

# --- terraform, in a COPY -------------------------------------------
#
# The copy is not tidiness. `init` REWRITES `.terraform.lock.hcl` with hashes
# from whichever registry the binary uses, so validating with OpenTofu in place
# would silently replace the terraform-registry hashes CI verifies against.

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cp -R "$TF_DIR/." "$WORK/"

tf_init() { (cd "$WORK" && "$TF_BIN" init -backend=false -input=false >/dev/null); }
tf_validate() { (cd "$WORK" && "$TF_BIN" validate >/dev/null); }
tf_test() { (cd "$WORK" && "$TF_BIN" test -no-color); }
tf_fmt() { (cd "$TF_DIR" && "$TF_BIN" fmt -check -recursive >/dev/null); }
tf_lock_untouched() {
  git -C "$ROOT_DIR" diff --quiet -- "iac/terraform/.terraform.lock.hcl"
}

arm "terraform init (no backend)" tf_init
arm "terraform validate" tf_validate
arm "terraform mocked tenant gateway tests" tf_test
arm "terraform fmt -check" tf_fmt
arm "the repo's .terraform.lock.hcl was not rewritten" tf_lock_untouched

# --- arms 5-6: serverless.yml resolves ----------------------------------------
#
# Placeholder values, deliberately syntactically real: `print` resolves every
# ${env:...} and reports an unresolved variable as an error, so a fake that is
# merely well-formed is enough and no real account is touched.

sls_print() {
  ( cd "$SLS_DIR" \
    && AWS_REGION=eu-west-1 \
       IMAGE_URI="123456789012.dkr.ecr.eu-west-1.amazonaws.com/x@sha256:$(printf '0%.0s' {1..64})" \
       SQS_QUEUE_URL="https://sqs.eu-west-1.amazonaws.com/123456789012/q" \
       SQS_QUEUE_ARN="arn:aws:sqs:eu-west-1:123456789012:q" \
       npx --yes serverless@3 print --config serverless.yml --stage dev > "$WORK/sls.out" 2> "$WORK/sls.err" )
}

# ANCHORED on the `name:` KEY and on end-of-line. Two failures this shape avoids: an
# unanchored grep for the api function also matches `foundry-workflow-dev-apiLambdaFunction`
# and half a dozen CloudFormation resource names, so it would pass on a config that
# declares no function at all; and the resolved name is emitted as
# `    name: foundry-workflow-dev-api`, not as a bare line, so a pattern anchored to line start
# WITHOUT the key matches nothing and reports a real config as declaring no functions.
sls_declares_api() { grep -qE "^ *name: ${SERVICE}-dev-api$" "$WORK/sls.out"; }

# ARM 6 REPLACES "the resolved config declares both functions", and the replacement is
# stronger rather than weaker. That arm asserted that a scheduled function named
# `drainOutbox` EXISTED. It did, and it did nothing: its entire body was
#
#   /opt/venv/bin/python -c "from ...handlers.outbox_drain import handle_scheduled_drain"
#
# which reads as importing the symbol and exiting.
#
# CORRECTION, MEASURED 2026-09-05: it did not import anything either. `image.command`
# sets the container CMD, and this image's ENTRYPOINT (`docker/entrypoint.sh`) did not
# name `"$@"`, so every argument was discarded and the HTTP API server started instead.
# Until that was fixed, THIS ARM WAS CHECKING A STRING THE RUNTIME THREW AWAY -- a
# correct classifier over an input with no effect. The entrypoint now execs an explicit
# command (`tests/shape/test_entrypoint.py` pins both directions), which is what makes
# the property below load-bearing. Left as a warning: a gate can be right about its
# input and still be measuring nothing, and nothing in the gate itself can tell you.
#
# So the old arm did not merely fail to catch the inert function -- it REQUIRED it.
# Deleting the function turned this gate red. That inversion is the part worth
# recording: the check a reader would credit for proving the drain was deployed was the
# thing holding an import-only Lambda in the config.
#
# This is the estate's "a step whose NAME asserts an action must not have an inert body"
# rule (the monorepo's `scripts/check_inert_step_claims.py`) on a surface
# that gate cannot reach: it enumerates `run:` steps in the MONOREPO's own
# `.github/workflows`, and a serverless function here is neither a `run:` step nor part
# of that repository's git index.
#
# The property is checked over the RESOLVED config rather than over `serverless.yml`'s
# source text, because `${env:...}` substitution and `serverless` defaults both happen
# between the two.
#
# NOTE HONESTLY WHAT THIS ARM DOES AND DOES NOT MEASURE TODAY. After `drainOutbox` was
# removed, NO function here carries an `image.command` at all, so the production half of
# this arm currently inspects zero commands. A probe that matched nothing is not a
# measured negative, so the arm carries INLINE CONTROLS in both directions -- six cases,
# including the exact argv the deleted `drainOutbox` shipped -- and FAILS if the
# classifier does not flag the offenders or wrongly flags the conforming ones. The arm's
# value is forward-looking: it goes red the day an import-only command comes back.
sls_no_inert_function_command() {
  /usr/bin/python3 - "$WORK/sls.out" <<'PYINERT'
import ast
import sys

try:
    import yaml
except ImportError:
    print(
        "PyYAML is not importable, so the resolved config could not be PARSED. This arm "
        "REFUSES rather than falling back to a text scan: a `command:` is a YAML list "
        "whose elements are re-wrapped across lines by the resolver, so a regex over "
        "raw text reads a folded scalar as several unrelated lines.",
        file=sys.stderr,
    )
    raise SystemExit(2)


def command_is_inert(command):
    """True when `command` runs `python -c <code>` and <code> only IMPORTS.

    Parsed with `ast`, never matched with a regex: `import x` inside a string, a
    comment, or a call argument is not a top-level import, and only a parse tells
    them apart. An empty program is inert too -- it is the limit case of this shape.
    A command that is not `python -c` at all (`-m`, a console script, no command)
    is NOT this gate's business and must not be flagged.
    """
    if not command:
        return False
    parts = [str(c) for c in command]
    if "-c" not in parts:
        return False
    index = parts.index("-c")
    if index + 1 >= len(parts):
        return False
    try:
        tree = ast.parse(parts[index + 1])
    except SyntaxError:
        return False
    if not tree.body:
        return True
    return all(isinstance(node, (ast.Import, ast.ImportFrom)) for node in tree.body)


# INLINE CONTROLS, BOTH DIRECTIONS. The first is the argv `drainOutbox` actually
# shipped here. The rest are near misses that must stay quiet, because a false red here
# names a real file and a real function, and the natural response to one is to rewrite
# the CONFIG rather than to doubt the detector.
CONTROLS = (
    (True, ["/opt/venv/bin/python", "-c",
            "from shepherd_workflow_service.handlers.outbox_drain import handle_scheduled_drain"]),
    (True, ["python", "-c", "import os"]),
    (False, ["/opt/venv/bin/python", "-c",
             "from shepherd_workflow_service.handlers.outbox_drain import handle_scheduled_drain\n"
             "handle_scheduled_drain(build_drain())"]),
    (False, ["/opt/venv/bin/python", "-m", "shepherd_workflow_service.handlers.outbox_drain"]),
    (False, ["python", "-c", "raise SystemExit(main())"]),
    (False, None),
)
control_failures = [
    f"expected inert={expected} got {command_is_inert(command)} for {command!r}"
    for expected, command in CONTROLS
    if command_is_inert(command) is not expected
]
if control_failures:
    for failure in control_failures:
        print(f"the inert-command classifier is broken: {failure}", file=sys.stderr)
    raise SystemExit(1)

doc = yaml.safe_load(open(sys.argv[1], encoding="utf-8"))
functions = (doc or {}).get("functions") or {}

# VISIBILITY. A resolved config with no functions at all would leave the loop below
# with nothing to say, and its silence would be a fact about the probe rather than
# about the config.
if not functions:
    print(
        "the resolved config declares no functions, so this arm never reached one; the "
        "absence it would otherwise report is an artifact of the probe",
        file=sys.stderr,
    )
    raise SystemExit(1)

offenders = []
inspected = 0
for name, spec in sorted(functions.items()):
    command = ((spec or {}).get("image") or {}).get("command")
    if command is None:
        continue
    inspected += 1
    if command_is_inert(command):
        offenders.append(
            f"function {name!r} is deployed with an import-only command "
            f"({command!r}): it imports the symbol and exits, so whatever its name "
            "asserts, it does nothing. Call it, or delete the function"
        )

if offenders:
    for offender in offenders:
        print(offender, file=sys.stderr)
    raise SystemExit(1)

print(
    f"[service-iac]   {len(functions)} resolved function(s), {inspected} with an image "
    f"command, 0 inert; 6 classifier controls held"
)
raise SystemExit(0)
PYINERT
}

arm "serverless print resolves the config" sls_print
arm "the resolved config declares the api function" sls_declares_api
arm "no resolved function is deployed with an import-only command" sls_no_inert_function_command

# --- arm 7: the workflow invokes serverless from the config's own directory ---

workflow_serverless_has_working_directory() {
  /usr/bin/python3 - "$WORKFLOW" <<'PYWF'
import sys

try:
    import yaml
except ImportError:
    print(
        "PyYAML is not importable, so this arm cannot read the workflow's structure. "
        "It REFUSES rather than falling back to a text scan: the text version of this "
        "arm reported the JOB NAME 'terraform validate + serverless print' as a "
        "serverless step with no working-directory -- a false red naming a real file "
        "and a real line, which is the direction that gets a correct workflow edited "
        "instead of a broken detector.",
        file=sys.stderr,
    )
    raise SystemExit(2)

doc = yaml.safe_load(open(sys.argv[1], encoding="utf-8"))

# Only a step's `run` COMMAND counts. Names, comments and job titles are prose
# ABOUT serverless, not invocations of it, and reading structure is the only
# thing that tells the two apart.
seen = 0
offenders = []
for job_name, job in (doc.get("jobs") or {}).items():
    job_dir = ((job.get("defaults") or {}).get("run") or {}).get("working-directory")
    for step in job.get("steps") or []:
        run = step.get("run") or ""
        if not any(w in run for w in ("serverless deploy", "serverless print", "serverless@3")):
            continue
        seen += 1
        if not (step.get("working-directory") or job_dir):
            offenders.append(f"{job_name}: {step.get('name') or run.splitlines()[0]}")

# POSITIVE CONTROL. A workflow with no serverless invocation at all leaves this
# arm an empty offender list -- byte-identical to a correct one. A green from a
# probe that matched nothing is not a measured negative.
if seen == 0:
    print(
        "no step in this workflow invokes serverless, so this arm matched nothing and its "
        "silence is an artifact of the probe rather than a property of the workflow",
        file=sys.stderr,
    )
    raise SystemExit(1)

if offenders:
    print(
        "serverless step(s) without a working-directory: " + "; ".join(offenders)
        + " -- `serverless --config <subdir>/serverless.yml` from a parent exits 1 with "
        "'Service configuration is expected to be placed in a root of a service'",
        file=sys.stderr,
    )
    raise SystemExit(1)
raise SystemExit(0)
PYWF
}

arm "the workflow runs serverless from the config's own directory" \
  workflow_serverless_has_working_directory

# --- verdict -----------------------------------------------------------------

if [ "$RAN" -lt "$EXPECTED_ARMS" ]; then
  echo "[service-iac] REFUSED: only $RAN arm(s) reached the accumulator; expected $EXPECTED_ARMS" >&2
  exit 2
fi

if [ "$FAILED" -gt 0 ]; then
  echo "[service-iac] FAIL: $FAILED of $RAN arm(s) red; all $RAN were executed" >&2
  exit 1
fi

echo "[service-iac] PASS: all $RAN arm(s) executed and green ($TF_BIN + serverless@3)"
