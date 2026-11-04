"""The scheduled process that RELAYS the workflow ledger's outbox.

Sibling of `main.py`, and a composition root for the same reason: it is allowed
to reach for adapters because it is the place where wiring happens. Everything
it calls takes its inputs explicitly.

WHAT THIS EXISTS TO FIX. `wf_outbox` is written transactionally by the vendored
`StageRunner` -- an event and the stage-run write commit together, which is the
whole point of an outbox -- and until this module existed nothing in this
service ever moved a row out again. `OutboxDispatcher` shipped in the vendored
package and was constructed nowhere. Rows accumulated with `sent_at` NULL
forever, so every stage transition this service decided was recorded and
delivered to nobody.

NOTHING RUNS AT IMPORT TIME, deliberately, and this is not a style choice. The
function this replaces was a Lambda whose entire body imported a symbol. If a
future command ever does that to this module by mistake, it must do NOTHING
visible rather than half-drain: an import that silently relays events is far
harder to notice than one that relays none.

HOW IT IS INVOKED. `python -m shepherd_workflow_service.ledger_drain_main`, as a
scheduled ECS task or any periodic runner. See `iac/serverless.yml` for why this
image's Lambda target cannot run it as a one-shot command today.
"""

from __future__ import annotations

import json
import sys

from shepherd_workflow_service.config import ServiceConfig
from shepherd_workflow_service.container import build_container


def run() -> int:
    """Drain once, print what happened, and return a process exit status.

    A FAILED row is not data loss and not an error in the usual sense -- it
    stayed pending and the next drain retries it -- but the exit status is
    non-zero anyway, because a schedule whose failures are invisible is a
    schedule nobody looks at. An AMBIGUOUS row exits zero: the dispatcher
    already chose a possible duplicate over a possible loss and recorded the
    ambiguity in the row itself.
    """
    config = ServiceConfig.from_environment()
    report = build_container(config).drain_ledger_outbox()
    sys.stdout.write(
        json.dumps(
            {
                "drain": "ledger_outbox",
                "published": report.published,
                "ambiguous": report.ambiguous,
                "failed": report.failed,
                "total": report.total,
            }
        )
        + "\n"
    )
    return 1 if report.failed else 0


if __name__ == "__main__":
    raise SystemExit(run())
