# One image, two runtimes -- see src/shepherd_workflow_service/main.py's docstring
# for the full explanation of how the same process satisfies both.
#
#   * ECS Fargate:   this image's CMD *is* the service. It binds $PORT and
#                     serves the ALB directly.
#   * Lambda (image
#     package type):  the AWS Lambda Web Adapter (the extra COPY --from
#                     below) intercepts each Lambda invocation, starts this
#                     exact CMD as a child process, and forwards the
#                     invocation to it as an HTTP request over localhost.
#
# Supply-chain discipline borrowed from an earlier backend's Dockerfile
# (the better of that repo's two images; its Lambda sibling diverged, which
# is the reason THIS project builds one image, not two):
#   * base pinned by digest, not a floating tag
#   * third-party deps installed with `pip install --require-hashes` against
#     a lockfile pip-compile generated (requirements/service.lock) -- this
#     package's OWN source is never pip-installed here; it is COPY'd
#     directly (see the runtime stage), so there is nothing local for a hash
#     to cover and nothing unpinned for a second, drifted install path to
#     sneak in
#   * explicit per-path COPY, never `COPY . .`
#   * chmod -R a=rX, then USER <numeric-uid>:<numeric-uid> -- no named user,
#     no shell, nothing world-writable
#
# `tests/shape/test_single_dockerfile.py` asserts there is exactly ONE
# Dockerfile in this package and that it installs from a hash-pinned
# lockfile -- the two-Dockerfile drift this borrows discipline from is a
# structural failure mode, not a one-off mistake, so the check makes a
# second Dockerfile appearing later mechanically impossible rather than a
# convention someone has to remember.

FROM python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea AS builder

ARG SOURCE_DATE_EPOCH=0

WORKDIR /build

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements/service.lock requirements/service.lock
RUN test "${SOURCE_DATE_EPOCH}" -ge 0 \
    && pip install --no-cache-dir --require-hashes -r requirements/service.lock

# --- runtime stage -----------------------------------------------------

FROM python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea AS runtime

# The Lambda Web Adapter: a static binary, inert unless the Lambda runtime
# invokes this container. Its presence costs nothing under ECS. Pinned by
# digest for the same reason the Python base is.
COPY --from=public.ecr.aws/awsguru/aws-lambda-adapter@sha256:e2653f741cd15851ba4f13f3cc47d29f2d14377c7d11737bfa272baa1b569007 /lambda-adapter /opt/extensions/lambda-adapter

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080 \
    AWS_LWA_PORT=8080 \
    DB_PATH=/tmp/workflow.db \
    WORKFLOW_DEFINITION_PATH=/app/contracts/workflow.v1.json

WORKDIR /app

# This package's own source -- COPY'd directly, never pip-installed, so
# `requirements/service.lock` only has to hash-pin genuine third-party
# dependencies. Explicit per-path, matching the discipline this borrows.
COPY vendor/shepherd_contracts shepherd_contracts
# The second vendored package, added when the service began importing it. It was
# vendored into the checkout without this line, so the image built and then died
# at import with `No module named 'shepherd_workflow'` -- an explicit per-path
# COPY buys supply-chain clarity at the price of exactly this failure whenever a
# new vendored package lands, which is the trade this Dockerfile intends.
COPY vendor/shepherd_workflow shepherd_workflow
# The workflow DEFINITION, not code. The service refuses to start without one,
# deliberately: this package owns no stage names, so a container with no
# definition has nothing to execute and should say so at startup rather than on
# the first request.
COPY contracts contracts
COPY src/shepherd_workflow_service shepherd_workflow_service
COPY docker/entrypoint.sh entrypoint.sh

RUN chmod -R a=rX /app \
    && chmod 0555 entrypoint.sh \
    && mkdir -p /tmp \
    && chmod 1777 /tmp

USER 65532:65532
EXPOSE 8080

ENTRYPOINT ["/app/entrypoint.sh"]
