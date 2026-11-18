"""Probe an immutable local image, signed auth and SQLite restart/restore.

Uses isolated Docker resources; never touches an existing service or volume.
Signing keys remain in memory. The receipt contains no tokens or private keys.
"""

import argparse
import hashlib
import json
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import uuid4

import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from verification_support import cleanup_docker, file_hashes, publish_receipt

ISSUER = "https://container-verification.example.invalid"
AUDIENCE = "shepherd-workflow-printables"


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def main():
    if not __debug__:
        raise RuntimeError("verification requires Python assertions enabled")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--definition", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    if args.receipt.exists():
        parser.error("receipt already exists; choose a new path to preserve earlier evidence")
    starting_hashes = file_hashes(
        [Path(__file__).resolve(), Path(__file__).with_name("verification_support.py").resolve()]
    )
    image = json.loads(docker("image", "inspect", args.image))[0]
    image_id = image["Id"]
    definition = args.definition.resolve()
    definition_bytes = definition.read_bytes()
    definition_sha256 = hashlib.sha256(definition_bytes).hexdigest()
    name = "wf-probe-proof-" + uuid4().hex[:10]
    volumes = [name + "-data", name + "-restore", name + "-config"]
    containers = []
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(private.public_key()))
    public.update(kid="probe", alg="RS256", use="sig")
    checks = []

    def token(**overrides):
        claims = {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "sub": "container-probe",
            "custom:org_id": "example-org",
            "scope": "jobs:write jobs:read",
            "iat": int(time.time()),
            "exp": int(time.time()) + 300,
        }
        claims.update(overrides)
        return jwt.encode(claims, private, algorithm="RS256", headers={"kid": "probe"})

    def request(origin, path, value=None, bearer=None, headers=None):
        fields = {"Content-Type": "application/json", **(headers or {})}
        if bearer:
            fields["Authorization"] = "Bearer " + bearer
        req = Request(
            origin + path,
            headers=fields,
            data=None if value is None else json.dumps(value).encode(),
        )
        try:
            with urlopen(req, timeout=5) as response:
                return response.status, json.load(response)
        except HTTPError as error:
            return error.code, json.load(error)

    def launch(label, volume):
        container = name + "-" + label
        containers.append(container)
        docker(
            "run",
            "-d",
            "--name",
            container,
            "--network",
            name,
            "--env",
            f"NO_PROXY={name}-jwks,localhost,127.0.0.1",
            "--env",
            f"no_proxy={name}-jwks,localhost,127.0.0.1",
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--memory=512m",
            "--cpus=1",
            "--pids-limit=128",
            "--tmpfs",
            "/tmp:rw,noexec,nosuid,size=16777216",
            "-p",
            "127.0.0.1::8080",
            "-v",
            volume + ":/data",
            "-v",
            volumes[2] + ":/run/workflow-config:ro",
            "-e",
            "SERVICE_ENV=production",
            "-e",
            "DB_PATH=/data/workflow.db",
            "-e",
            "WORKFLOW_DEFINITION_PATH=/run/workflow-config/workflow.json",
            "-e",
            "WORKFLOW_TABLE_PREFIX=printable_probe_",
            "-e",
            "PRINCIPAL_ISSUER=" + ISSUER,
            "-e",
            "AUTH_AUDIENCE=" + AUDIENCE,
            "-e",
            "AUTH_JWKS_URI=http://" + name + "-jwks:8081/jwks.json",
            image_id,
        )
        port = docker("port", container, "8080/tcp").split(":")[-1]
        origin = "http://127.0.0.1:" + port
        for _ in range(60):
            try:
                if request(origin, "/readyz")[0] == 200:
                    return container, origin
            except (URLError, TimeoutError, ConnectionError):
                pass
            time.sleep(0.5)
        raise RuntimeError("container readiness deadline exceeded")

    try:
        docker("network", "create", name)
        for volume in volumes:
            docker("volume", "create", volume)
            docker(
                "run",
                "--rm",
                "--user=0",
                "-v",
                volume + ":/data",
                "--entrypoint",
                "sh",
                image_id,
                "-c",
                "chown 65532:65532 /data; chmod 700 /data",
            )
        # Copy public configuration into a volume: works with local and remote Docker.
        with tempfile.TemporaryDirectory(prefix=name, dir=Path.home()) as temp:
            root = Path(temp)
            root.chmod(0o755)
            definition = root / "workflow.json"
            definition.write_bytes(definition_bytes)
            definition.chmod(0o444)
            public_directory = root / "public"
            public_directory.mkdir(mode=0o755)
            (public_directory / "jwks.json").write_text(json.dumps({"keys": [public]}))
            (public_directory / "jwks.json").chmod(0o644)
            writer = name + "-config-writer"
            containers.append(writer)
            docker("create", "--name", writer, "-v", volumes[2] + ":/config",
                   "--entrypoint", "python", image_id, "-c", "pass")
            docker("cp", str(definition), writer + ":/config/workflow.json")
            docker("cp", str(public_directory / "jwks.json"), writer + ":/config/jwks.json")
            docker("rm", writer)
            containers.append(name + "-jwks")
            docker(
                "run",
                "-d",
                "--name",
                containers[-1],
                "--network",
                name,
                "--read-only",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "-v",
                volumes[2] + ":/public:ro",
                "--entrypoint",
                "python",
                image_id,
                "-m",
                "http.server",
                "8081",
                "--directory",
                "/public",
            )
            container, origin = launch("service", volumes[0])
            docker(
                "exec",
                container,
                "python",
                "-c",
                "import jwt; keys=jwt.PyJWKClient('http://"
                + name
                + "-jwks:8081/jwks.json').fetch_data(); assert len(keys['keys']) == 1",
            )
            body = {"job_id": "durable-probe", "input_digest": "source-digest"}
            for label, bearer in [
                ("expired", token(exp=1)),
                ("wrong-audience", token(aud="other")),
                ("wrong-issuer", token(iss="other")),
                ("missing-scope", token(scope="jobs:read")),
                ("forged-signature", token().rsplit(".", 1)[0] + ".Zm9yZ2Vk"),
            ]:
                assert request(origin, "/v1/jobs", body, bearer)[0] == 401, label
                checks.append(label + "-refused")
            assert (
                request(
                    origin,
                    "/v1/jobs",
                    body,
                    headers={
                        "x-org-id": "example-org",
                        "x-subject-id": "probe",
                        "x-scopes": "jobs:write",
                    },
                )[0]
                == 401
            )
            checks.append("unsigned-identity-refused")
            status, started = request(origin, "/v1/jobs", body, token())
            assert status == 201, (status, started)
            stage_path = "/v1/stage-runs/" + started["stage_run_id"]
            assert request(origin, stage_path, bearer=token(**{"custom:org_id": "other"}))[0] == 404
            checks.append("cross-organization-read-hidden")
            status, leased = request(
                origin, stage_path + "/commands", {"kind": "lease", "state_version": 0}, token()
            )
            assert status == 200 and leased["status"] == "leased"
            docker("restart", container)
            origin = "http://127.0.0.1:" + docker("port", container, "8080/tcp").split(":")[-1]
            restored = None
            for _ in range(60):
                try:
                    status, restored = request(origin, stage_path, bearer=token())
                    if status == 200:
                        break
                except (URLError, TimeoutError, ConnectionError):
                    pass
                time.sleep(0.5)
            assert restored == leased, "restarted endpoint did not return the retained lease"
            checks.append("leased-stage-survives-restart")
            command = {
                "kind": "complete",
                "state_version": leased["state_version"],
                "fence": leased["fence"],
                "payload": {"payload_digest": "artifact-digest"},
            }
            status, finished = request(origin, stage_path + "/commands", command, token())
            assert status == 200 and finished["status"] == "succeeded"
            assert request(origin, stage_path + "/commands", command, token())[0] == 409
            checks.append("duplicate-completion-refused")
            docker(
                "exec",
                container,
                "python",
                "-c",
                "import sqlite3; s=sqlite3.connect('/data/workflow.db'); "
                "d=sqlite3.connect('/data/backup.db'); s.backup(d); "
                "assert d.execute('PRAGMA integrity_check').fetchone()[0]=='ok'; d.close()",
            )
            docker(
                "run",
                "--rm",
                "--user=0",
                "-v",
                volumes[0] + ":/source:ro",
                "-v",
                volumes[1] + ":/restore",
                "--entrypoint",
                "sh",
                image_id,
                "-c",
                "cp /source/backup.db /restore/workflow.db; "
                "chown 65532:65532 /restore/workflow.db; chmod 600 /restore/workflow.db",
            )
            _, recovery = launch("restored", volumes[1])
            assert request(recovery, stage_path, bearer=token()) == (200, finished)
            assert request(recovery, "/v1/jobs", body, token())[1] == finished
            checks.append("online-backup-restores-identical-terminal-stage")
            checks.append("restored-start-replays-existing-stage")
            evidence = {
                "scope": "local-container-only",
                "image_id": image_id,
                "architecture": image["Architecture"],
                "definition_sha256": definition_sha256,
                "probe_sha256": starting_hashes[str(Path(__file__).resolve())],
                "support_sha256": starting_hashes[
                    str(Path(__file__).with_name("verification_support.py").resolve())
                ],
                "checks": checks,
                "stage_run_id": finished["stage_run_id"],
                "final_state_version": finished["state_version"],
            }
    finally:
        cleanup = cleanup_docker(containers=containers, volumes=volumes, networks=[name])
    evidence["cleanup"] = cleanup
    publish_receipt(args.receipt, evidence, starting_hashes)
    print(json.dumps({"passed": checks, "receipt": str(args.receipt)}))


if __name__ == "__main__":
    main()
