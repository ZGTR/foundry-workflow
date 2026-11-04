"""Non-HTTP inbound entrypoints: queue consumers, scheduled jobs. Same rule as
`controllers/`: parse, call a service, done. No business logic here.
"""

from __future__ import annotations
