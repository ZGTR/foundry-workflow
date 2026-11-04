"""Guards against the exact failure mode an earlier backend repository lived
with: a second Dockerfile (`Dockerfile.lambda`) that quietly diverged from
the first -- different base image, installed the UNPINNED requirements file
instead of the hash-checked lock, ran as root. Nothing detected that until
it was noticed by hand.

This package's whole argument is that one
image serves both targets, so there is no second Dockerfile to drift in the
first place. These tests make that structural, not a convention someone has
to remember: exactly one Dockerfile must exist, and it must install its
third-party dependencies with `--require-hashes` against a lockfile where
every dependency actually carries a hash.
"""

from __future__ import annotations

import re
from pathlib import Path

PACKAGE_ROOT = Path(__file__).parent.parent.parent
LOCKFILE = PACKAGE_ROOT / "requirements/service.lock"


_DOCKERFILE_PATTERN = re.compile(r"(^|[./])Dockerfile($|\.)")


def _dockerfiles(root: Path) -> list[Path]:
    """Every file under `root` that IS a Dockerfile or claims to be a variant
    of one -- `Dockerfile`, `Dockerfile.lambda`, `foo.Dockerfile`, etc.
    Deliberately broad: the shepherd incident's second file was named
    `Dockerfile.lambda`, not `Dockerfile`, so a check that only looked for
    the exact name `Dockerfile` would have missed exactly the case it exists
    to catch.
    """
    return sorted(
        p
        for p in root.rglob("*")
        if p.is_file() and _DOCKERFILE_PATTERN.search(p.name) and ".git" not in p.parts
    )


def test_exactly_one_dockerfile_exists() -> None:
    dockerfiles = _dockerfiles(PACKAGE_ROOT)
    assert dockerfiles == [PACKAGE_ROOT / "Dockerfile"], (
        f"expected exactly one Dockerfile at the package root, found: {dockerfiles}. "
        "A second Dockerfile (e.g. a Lambda-specific one) is exactly the drift this "
        "template exists to rule out -- see README.md's Motivation section."
    )


def test_check_goes_red_on_a_second_dockerfile(tmp_path: Path) -> None:
    (tmp_path / "Dockerfile").write_text("FROM scratch\n")
    (tmp_path / "Dockerfile.lambda").write_text("FROM scratch\n")

    found = _dockerfiles(tmp_path)

    assert len(found) == 2, "the detector itself must be able to see a second Dockerfile"


def test_dockerfile_installs_with_require_hashes() -> None:
    dockerfile = (PACKAGE_ROOT / "Dockerfile").read_text()
    assert "--require-hashes" in dockerfile, (
        "Dockerfile must install third-party dependencies with --require-hashes -- "
        "the shepherd Lambda image installed the unpinned requirements.txt instead "
        "of the hash-checked .lock, and nothing caught that two images had drifted."
    )
    assert "requirements.txt" not in dockerfile, (
        "the Dockerfile must reference only the hash-pinned .lock file, never an "
        "unpinned .txt/.in requirements file, as a second unhashed install path."
    )


def test_the_lockfile_the_dockerfile_installs_from_exists_and_is_referenced() -> None:
    dockerfile = (PACKAGE_ROOT / "Dockerfile").read_text()
    assert "requirements/service.lock" in dockerfile
    assert LOCKFILE.is_file(), f"{LOCKFILE} is referenced by Dockerfile but does not exist"


def _dependency_records(lock_text: str) -> list[str]:
    """Split a pip-compile --generate-hashes lockfile into one string per
    dependency (a top-level, non-indented, non-comment line plus every
    indented continuation line that follows it)."""
    records: list[str] = []
    current: list[str] = []
    for line in lock_text.splitlines():
        if line.startswith("#"):
            continue
        if line and not line[0].isspace():
            if current:
                records.append("\n".join(current))
            current = [line]
        elif current:
            current.append(line)
    if current:
        records.append("\n".join(current))
    return records


def test_every_dependency_in_the_lockfile_is_hash_pinned() -> None:
    records = _dependency_records(LOCKFILE.read_text())
    assert records, "lockfile parsed to zero dependency records -- parser or fixture is broken"

    unpinned = [r.splitlines()[0] for r in records if "--hash=sha256:" not in r]
    assert unpinned == [], f"dependencies with no --hash=sha256: entry: {unpinned}"


def test_check_goes_red_on_an_unhashed_dependency() -> None:
    fake_lock = (
        "requests==2.31.0\n"
        "    # via -r requirements.in\n"
        "pydantic==2.7.0 \\\n"
        "    --hash=sha256:abc123\n"
    )

    records = _dependency_records(fake_lock)
    unpinned = [r.splitlines()[0] for r in records if "--hash=sha256:" not in r]

    assert unpinned == ["requests==2.31.0"]


def test_lockfile_coverage_check_still_catches_a_genuinely_absent_requirement() -> None:
    """The red direction, so stripping extras cannot quietly disarm the check."""
    lock_names = {
        r.splitlines()[0].split("==")[0] for r in _dependency_records(LOCKFILE.read_text())
    }
    fabricated = ["definitely-not-a-real-package[extra]", "another-absent-one"]
    missing = [name.split("[")[0] for name in fabricated if name.split("[")[0] not in lock_names]
    assert missing == [n.split("[")[0] for n in fabricated], (
        "the extras-stripping comparison must still report absent packages"
    )


def test_lockfile_covers_every_top_level_requirement() -> None:
    requirements_in = (PACKAGE_ROOT / "requirements/service.in").read_text()
    # Strip EXTRAS before comparing: `pyjwt[crypto]` locks as `pyjwt`, plus the
    # extra's own transitive pins. Without this the check reported `pyjwt[crypto]`
    # missing from a lockfile that pins it correctly -- a false red on a green
    # supply chain, which invites "fixing" the lockfile that was already right.
    top_level = [
        line.split(">=")[0].split("==")[0].split("[")[0].strip()
        for line in requirements_in.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    lock_names = {
        r.splitlines()[0].split("==")[0] for r in _dependency_records(LOCKFILE.read_text())
    }

    missing = [name for name in top_level if name not in lock_names]
    assert missing == [], f"top-level requirement(s) missing from the lockfile: {missing}"
