"""SQLAlchemy adapter. Optional: ``pip install shepherd-workflow[sql]``.

Kept behind a subpackage and an optional extra so the core package imports with
no database driver present. A library that cannot be imported without SQLAlchemy
has made a storage choice on its consumer's behalf.
"""

from shepherd_workflow.sql.repository import (
    SqlAlchemyOutbox,
    SqlAlchemyStageRepository,
    SqlAlchemyUnitOfWork,
)
from shepherd_workflow.sql.schema import build_tables

__all__ = [
    "SqlAlchemyOutbox",
    "SqlAlchemyStageRepository",
    "SqlAlchemyUnitOfWork",
    "build_tables",
]
