#!/usr/bin/env sh
# The container's ENTRYPOINT. Detects which runtime it is under and always
# execs the SAME command -- src/shepherd_workflow_service/main.py -- because the
# whole point of "one image, two runtimes" (Dockerfile's docstring) is that
# nothing here forks the launch path. What differs is only:
#
#   * whether the Lambda Web Adapter extension is the thing that started
#     this script (Lambda invokes the adapter, not this script, first) --
#     that is entirely outside this script's control and requires no branch
#     here;
#   * the RUNTIME_MODE value this script exports for ServiceConfig to read,
#     used only for things like log formatting, never to change what runs.
#
# `tests/shape/test_entrypoint.py` drives this file directly under both
# conditions and asserts the detection + export, with `python` replaced by a
# stub so the test never needs a real interpreter or app boot.
set -eu

RUNTIME_PYTHON="${ENTRYPOINT_PYTHON:-python}"

if [ -n "${AWS_LAMBDA_FUNCTION_NAME:-}" ]; then
    echo "entrypoint: AWS_LAMBDA_FUNCTION_NAME=${AWS_LAMBDA_FUNCTION_NAME} -- running under Lambda; the Lambda Web Adapter forwards invocations to this process over HTTP" >&2
    export RUNTIME_MODE="lambda"
else
    echo "entrypoint: no Lambda runtime detected -- running as an ordinary ECS/local HTTP service" >&2
    export RUNTIME_MODE="${RUNTIME_MODE:-http}"
fi

# AN EXPLICIT COMMAND WINS. Without this branch the image has exactly one launch
# path and every `image.command` (serverless) or `command:` (ECS task definition)
# is DISCARDED: the CMD arrives here as "$@", and a script that never names "$@"
# throws it away.
#
# That is not hypothetical. The deleted `drainOutbox` Lambda shipped
#   [/opt/venv/bin/python, -c, "from ...handlers.outbox_drain import handle_scheduled_drain"]
# and was recorded, in `iac/serverless.yml` and in `blackboxes/workflow-ledger/proof.md`,
# as "imports the symbol and exits". It did neither. The arguments landed here and
# were dropped, so what ran 1440 times a day was the HTTP API server. Measured
# 2026-09-05 by running this script with those exact arguments against a stub
# interpreter: the argv printed was `-m shepherd_workflow_service.main`, unchanged.
#
# RUNTIME_MODE is exported above, so a one-shot command inherits the same runtime
# detection the service does.
if [ "$#" -gt 0 ]; then
  echo "entrypoint: explicit command given -- running it instead of the HTTP service: $*" >&2
  exec "$@"
fi

exec "${RUNTIME_PYTHON}" -m shepherd_workflow_service.main
