"""Edge-case coverage for pyjobkit-simulate."""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
import types
from pathlib import Path

import pytest

from pyjobkit.cli_simulate import (
    SimulateError,
    _load_executor_factory,
    _load_input,
    main,
)


def test_load_input_rejects_non_list_jobs(tmp_path: Path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"jobs": "not a list"}))
    with pytest.raises(SimulateError, match="'jobs' must be a list"):
        _load_input(bad)


def test_load_input_rejects_missing_jobs_key(tmp_path: Path) -> None:
    bad = tmp_path / "missing.json"
    bad.write_text(json.dumps({"things": []}))
    with pytest.raises(SimulateError, match="top-level 'jobs'"):
        _load_input(bad)


def test_load_input_yaml_requires_pyyaml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "x.yaml"
    path.write_text("jobs: []\n")
    monkeypatch.setitem(sys.modules, "yaml", None)  # type: ignore[arg-type]
    with pytest.raises(SimulateError, match="PyYAML"):
        _load_input(path)


def test_load_input_yaml_when_pyyaml_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "x.yaml"
    path.write_text("jobs:\n  - kind: noop\n")

    fake_yaml = types.ModuleType("yaml")
    fake_yaml.safe_load = lambda text: {"jobs": [{"kind": "noop"}]}  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "yaml", fake_yaml)
    out = _load_input(path)
    assert out == {"jobs": [{"kind": "noop"}]}


def test_load_executor_factory_rejects_bad_dotted() -> None:
    with pytest.raises(SimulateError, match="module:attr"):
        _load_executor_factory("no_colon")


def test_load_executor_factory_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = types.ModuleType("tests.fake_simulate_exec")

    class _Exec:
        kind = "ok"

    mod.Factory = lambda: _Exec()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, mod.__name__, mod)
    assert isinstance(_load_executor_factory(f"{mod.__name__}:Factory"), _Exec)


def test_simulate_propagates_retry_policy(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Branch where ``retry_policy`` is present in the spec."""

    bad = tmp_path / "rp.json"
    bad.write_text(
        json.dumps(
            {
                "jobs": [
                    {
                        "kind": "subprocess",
                        "payload": {"cmd": "echo hi"},
                        "retry_policy": "fixed:0.1",
                    }
                ]
            }
        )
    )
    with pytest.raises(SystemExit) as excinfo:
        main([str(bad), "--concurrency", "1", "--timeout", "10"])
    assert excinfo.value.code == 0


def test_simulate_main_handles_simulate_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"jobs": [{"missing": "kind"}]}))
    with pytest.raises(SystemExit) as excinfo:
        main([str(bad)])
    assert excinfo.value.code == 2
    assert "pyjobkit-simulate" in capsys.readouterr().err


def test_simulate_main_handles_missing_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main([str(tmp_path / "does_not_exist.json")])
    assert excinfo.value.code == 2


def test_simulate_main_load_executor_via_dotted_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Smoke-test the --executor argv path."""

    mod = types.ModuleType("tests.fake_sim_executor_path")

    class _PluginExec:
        kind = "ext"

        async def run(self, *, job_id, payload, ctx):
            return {}

    mod.Factory = lambda: _PluginExec()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, mod.__name__, mod)

    bad = tmp_path / "ext.json"
    bad.write_text(json.dumps({"jobs": [{"kind": "ext", "payload": {}}]}))
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                str(bad),
                "--executor",
                f"{mod.__name__}:Factory",
                "--timeout",
                "5",
            ]
        )
    assert excinfo.value.code == 0
