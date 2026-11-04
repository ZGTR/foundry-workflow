"""Domain-owned data-access contracts.

These are Protocols, like `ports/`, but categorised separately because they
belong to this service's own ubiquitous language (a "stage repository" is a
domain concept) rather than to an external integration (a queue, an auth
header). `services/` depends on the Protocol here, never on a concrete
`adapters/real/*` or `adapters/fake/*` class.
"""

from __future__ import annotations
