"""Tests for the pyjobkit-simulate CLI (#69)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pyjobkit.cli_simulate import SimulateError, main


def _write(tmp_path: Path, body: dict) -> Path:
    path = tmp_path / "jobs.json"
    path.write_text(json.dumps(body), encoding="utf-8")
    return path


def test_simulate_runs_subprocess_jobs(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    spec = _write(
        tmp_path,
        {
            "jobs": [
                {"kind": "subprocess", "payload": {"cmd": "echo hi"}},
                {
                    "kind": "subprocess",
                    "payload": {"cmd": "echo hi", "shadow_safe": True},
                    "shadow": True,
                },
            ]
        },
    )
    with pytest.raises(SystemExit) as excinfo:
        main([str(spec), "--concurrency", "2", "--timeout", "10"])
    assert excinfo.value.code == 0
    out = capsys.readouterr().out
    summary = json.loads(out)["summary"]
    assert len(summary["success"]) == 2


def test_simulate_rejects_missing_jobs_key(tmp_path: Path) -> None:
    spec = tmp_path / "bad.json"
    spec.write_text(json.dumps({"not_jobs": []}), encoding="utf-8")
    with pytest.raises(SystemExit) as excinfo:
        main([str(spec)])
    assert excinfo.value.code == 2


def test_simulate_rejects_missing_file(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main([str(tmp_path / "nope.json")])
    assert excinfo.value.code == 2


def test_simulate_validates_job_entry(tmp_path: Path) -> None:
    spec = _write(tmp_path, {"jobs": [{"payload": {}}]})
    with pytest.raises(SystemExit) as excinfo:
        main([str(spec)])
    assert excinfo.value.code == 2
