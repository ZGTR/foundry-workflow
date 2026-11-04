"""Business rules. Depends on `ports/`, `repositories/` and `models/` only --
never on a concrete `adapters/*` class. `tests/shape/test_layering.py`
enforces this by import-scanning every module here.
"""

from __future__ import annotations
