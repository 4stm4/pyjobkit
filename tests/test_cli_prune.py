"""Unit tests for pyjobkit-prune argument parsing."""

from __future__ import annotations

from datetime import timedelta

import argparse
import pytest

from pyjobkit.cli_prune import _parse_age, _parse_statuses


@pytest.mark.parametrize(
    "spec,expected",
    [
        ("30d", timedelta(days=30)),
        ("24h", timedelta(hours=24)),
        ("90m", timedelta(minutes=90)),
        ("60s", timedelta(seconds=60)),
        ("0.5d", timedelta(hours=12)),
        ("120", timedelta(seconds=120)),
    ],
)
def test_parse_age_accepted(spec: str, expected: timedelta) -> None:
    assert _parse_age(spec) == expected


@pytest.mark.parametrize("spec", ["", "abc", "10w", "1.2.3d"])
def test_parse_age_rejects_bad(spec: str) -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_age(spec)


def test_parse_statuses_validates() -> None:
    assert _parse_statuses("failed,timeout") == ("failed", "timeout")
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_statuses("failed,bogus")


def test_main_requires_dsn(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    from pyjobkit import cli_prune

    monkeypatch.delenv("PYJOBKIT_DSN", raising=False)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit, match="DSN required"):
        cli_prune.main(["--log-level", "ERROR"])


def test_main_runs_and_prints_count(
    monkeypatch: pytest.MonkeyPatch, tmp_path, capsys: pytest.CaptureFixture[str]
) -> None:
    from pyjobkit import cli_prune

    # Stub out the SQL stack so we don't need a real DB.
    class _FakeBackend:
        def __init__(self, *_a, **_kw) -> None:
            pass

    class _FakeEngine:
        def __init__(self, *_a, **_kw) -> None:
            pass

        async def purge_finished(self, **_kw) -> int:
            return 42

    class _FakeAsyncEngine:
        async def dispose(self) -> None:
            pass

    def fake_create_engine(_dsn: str) -> _FakeAsyncEngine:
        return _FakeAsyncEngine()

    monkeypatch.setattr(cli_prune, "SQLBackend", _FakeBackend)
    monkeypatch.setattr(cli_prune, "Engine", _FakeEngine)
    monkeypatch.setattr(cli_prune, "create_async_engine", fake_create_engine)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as excinfo:
        cli_prune.main(
            [
                "--dsn", "sqlite://",
                "--older-than", "1d",
                "--statuses", "failed,timeout",
                "--log-level", "ERROR",
            ]
        )
    assert excinfo.value.code == 0
    out = capsys.readouterr().out.strip().splitlines()[-1]
    assert out == "42"
