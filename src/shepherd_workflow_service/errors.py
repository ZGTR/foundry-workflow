"""The workflow ledger's own errors.

All extend `shepherd_contracts.errors.ShepherdError` -- callers of this service catch the
shared vocabulary, never a bespoke exception type per package.

Every class here names a REFUSAL, which is an answer rather than a fault: each
maps to a 4xx in `app_factory._STATUS_BY_ERROR`, never to a 5xx. A caller that
cannot tell "the ledger declined this command" from "this service broke" cannot
decide whether retrying is correct.
"""

from __future__ import annotations

from shepherd_contracts.errors import ShepherdError


class StageRunNotFound(ShepherdError):
    """No stage run with this id is visible to this caller.

    Visible, not existing. Another org's stage run raises this too, so a caller
    cannot tell "there is no such id" from "there is one and it is not yours".
    """


class StageRunConflict(ShepherdError):
    """The ledger was not in the state the command assumed. 409; re-read and retry.

    `str(exc)` is the refusal class verbatim -- `state_version_conflict`,
    `stale_fence` -- because `app_factory` puts it in the response `detail`, and
    an operator triaging a 409 needs the class, not a paraphrase of it.
    """


class StageRunRejected(ShepherdError):
    """The command could not be applied and retrying it unchanged will not help. 422."""
