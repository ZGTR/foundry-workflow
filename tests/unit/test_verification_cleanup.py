"""Verification cannot report success while cleanup or source identity is uncertain."""

import subprocess
from types import SimpleNamespace

import pytest

from deploy.standalone import verification_support as support


def test_cleanup_refuses_unowned_names(monkeypatch):
    monkeypatch.setattr(support.subprocess, "run", lambda *a, **k: pytest.fail("must not remove"))
    with pytest.raises(ValueError, match="namespace"):
        support.cleanup_docker(containers=["existing-production"])


def test_successful_remove_is_not_enough_when_container_remains(monkeypatch):
    monkeypatch.setattr(support.subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=0))
    monkeypatch.setattr(support.subprocess, "check_output", lambda *a, **k: "wf-probe-owned\n")
    with pytest.raises(RuntimeError, match="cleanup incomplete"):
        support.cleanup_docker(containers=["wf-probe-owned"])


def test_daemon_failure_does_not_mean_resource_absence(monkeypatch):
    monkeypatch.setattr(support.subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=1))

    def unavailable(*args, **kwargs):
        raise subprocess.CalledProcessError(1, ["docker", "ps"])

    monkeypatch.setattr(support.subprocess, "check_output", unavailable)
    with pytest.raises(subprocess.CalledProcessError):
        support.cleanup_docker(containers=["wf-probe-owned"])


def test_already_absent_resource_is_verified_after_remove_error(monkeypatch):
    monkeypatch.setattr(support.subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=1))
    monkeypatch.setattr(support.subprocess, "check_output", lambda *a, **k: "unrelated\n")
    result = support.cleanup_docker(volumes=["wf-probe-owned"])
    assert result["volume"]["verified_absent"]
    assert result["volume"]["remove_exit_codes"]["wf-probe-owned"] == 1


def test_changed_probe_cannot_publish_receipt(tmp_path):
    source = tmp_path / "source.py"
    source.write_text("original")
    starting = support.file_hashes([source])
    source.write_text("changed")
    receipt = tmp_path / "receipt.json"
    with pytest.raises(RuntimeError, match="source changed"):
        support.publish_receipt(receipt, {"status": "passed"}, starting)
    assert not receipt.exists()


def test_existing_receipt_is_never_overwritten(tmp_path):
    source = tmp_path / "source.py"
    source.write_text("original")
    receipt = tmp_path / "receipt.json"
    receipt.write_text("retained")
    with pytest.raises(FileExistsError):
        support.publish_receipt(receipt, {"status": "passed"}, support.file_hashes([source]))
    assert receipt.read_text() == "retained"
