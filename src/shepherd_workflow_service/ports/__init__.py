"""Outbound contracts. Protocol definitions ONLY -- no `boto3`, no `sqlalchemy`,
no `requests`, no concrete implementation. `tests/shape/test_layering.py`
enforces this by import-scanning every module in this package.
"""

from __future__ import annotations
