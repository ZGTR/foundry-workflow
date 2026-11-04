"""Domain events: what happened, in this service's own vocabulary.

Distinct from `ports/event_bus.py`, which defines how an event travels. This
module defines what a `StageRunAdvanced` event *is*; the port and its adapters
don't know this domain exists.
"""

from __future__ import annotations
