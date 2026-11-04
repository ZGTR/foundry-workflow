"""HTTP routes. Parse, mint a principal, call a service, serialise -- nothing
else. Every handler here is under 12 lines, enforced by
`tests/shape/test_layering.py::test_route_handlers_are_thin`. Domain errors
are translated to HTTP status codes by exception handlers in `app_factory.py`,
which is what keeps the handlers themselves free of try/except.
"""

from __future__ import annotations
