"""Adapters implement `ports/` and `repositories/` Protocols.

`real/` talks to actual infrastructure (SQLite, SQS, HTTP headers). `fake/`
is in-memory and network-free; every service test outside `tests/integration`
that needs a port uses a fake, never a real adapter with a mock socket.
"""

from __future__ import annotations
