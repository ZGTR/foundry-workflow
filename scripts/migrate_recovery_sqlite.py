"""Add nullable recovery columns to an existing local SQLite ledger."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

COLUMNS = {
    "next_attempt_at": "DATETIME",
    "artifact_digest": "VARCHAR(64)",
    "output_ref": "VARCHAR(2048)",
    "next_stage_run_id": "VARCHAR(64)",
}


def migrate(path: Path, prefix: str) -> None:
    if not path.is_file():
        raise ValueError("Migration requires an existing SQLite database")
    if not prefix.replace("_", "").isalnum() and prefix != "":
        raise ValueError("Invalid table prefix")
    table = prefix + "stage_runs"
    with sqlite3.connect(path) as connection:
        existing = {row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')}
        if not existing:
            raise ValueError("Stage ledger table does not exist")
        for column, kind in COLUMNS.items():
            if column not in existing:
                connection.execute(f'ALTER TABLE "{table}" ADD COLUMN "{column}" {kind}')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", required=True, type=Path)
    parser.add_argument("--table-prefix", default="wf_")
    args = parser.parse_args()
    migrate(args.db_path, args.table_prefix)
    print("SQLite recovery columns present; existing rows preserved.")


if __name__ == "__main__":
    main()
