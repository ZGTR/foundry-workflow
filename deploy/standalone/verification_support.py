"""Receipt and cleanup checks shared by isolated local verification commands."""

import hashlib
import json
import subprocess
from pathlib import Path


def file_hashes(paths):
    return {str(path): hashlib.sha256(Path(path).read_bytes()).hexdigest() for path in paths}


def cleanup_docker(containers=(), volumes=(), networks=()):
    groups = {"container": tuple(containers), "volume": tuple(volumes), "network": tuple(networks)}
    if any(not name.startswith("wf-probe-") for names in groups.values() for name in names):
        raise ValueError("refusing cleanup outside the isolated verification namespace")
    result = {}
    for kind, names in groups.items():
        removals = {}
        for name in reversed(names):
            command = (
                ["docker", "rm", "-f", name]
                if kind == "container"
                else ["docker", kind, "rm", name]
            )
            removals[name] = subprocess.run(command, capture_output=True, check=False).returncode
        if not names:
            continue
        listing = (
            ["docker", "ps", "-a", "--format", "{{.Names}}"]
            if kind == "container"
            else ["docker", kind, "ls", "--format", "{{.Name}}"]
        )
        remaining = set(subprocess.check_output(listing, text=True).splitlines()) & set(names)
        if remaining:
            raise RuntimeError(f"owned {kind} cleanup incomplete: {sorted(remaining)}")
        result[kind] = {"remove_exit_codes": removals, "verified_absent": True}
    return result


def publish_receipt(path, data, starting_hashes):
    if file_hashes(starting_hashes) != starting_hashes:
        raise RuntimeError("verification source changed during execution; receipt refused")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)
        handle.write("\n")
