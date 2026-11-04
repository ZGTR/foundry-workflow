"""The recovered service owns exactly one pinned legacy engine source tree."""

import hashlib
import json
from pathlib import Path


def test_recovery_source_matches_provenance_pin():
    root = Path(__file__).resolve().parents[2]
    provenance = json.loads((root / "vendor/workflow-provenance.json").read_text())
    actual = {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((root / provenance["source_directory"]).rglob("*.py"))
    }
    assert actual == provenance["sha256"]
    assert provenance["upstream_revision"] == "99750e135db4ae5bc8ed5645ec5cedf7b67e9ed0"
