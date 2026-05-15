"""Tests for the pyjobkit-migrate console script."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from pyjobkit import cli_migrate


_ALEMBIC_AVAILABLE = importlib.util.find_spec("alembic") is not None


def test_main_requires_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PYJOBKIT_DSN", raising=False)
    monkeypatch.chdir(Path(__file__).parent)
    with pytest.raises(SystemExit, match="DSN required"):
        cli_migrate.main(["up"])


def test_alembic_config_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "alembic.config", None)  # type: ignore[arg-type]
    with pytest.raises(SystemExit, match="alembic"):
        cli_migrate._alembic_config("sqlite:///:memory:")


@pytest.mark.skipif(not _ALEMBIC_AVAILABLE, reason="alembic not installed")
def test_main_up_current_history_against_sqlite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Smoke-test the full happy path against a throwaway SQLite file."""

    monkeypatch.delenv("PYJOBKIT_DSN", raising=False)
    monkeypatch.chdir(tmp_path)
    db = tmp_path / "pyjobkit.db"
    dsn = f"sqlite:///{db}"

    cli_migrate.main(["--dsn", dsn, "up"])
    assert db.exists(), "migrate up should create the SQLite file"

    cli_migrate.main(["--dsn", dsn, "current"])
    cli_migrate.main(["--dsn", dsn, "history"])
    capsys.readouterr()  # drain alembic output

    # down with explicit step count.
    cli_migrate.main(["--dsn", dsn, "down", "-1"])
    cli_migrate.main(["--dsn", dsn, "down"])  # default step
    capsys.readouterr()


@pytest.mark.skipif(not _ALEMBIC_AVAILABLE, reason="alembic not installed")
def test_main_rewrites_async_dsn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Async DSN forms must be translated to the sync driver."""

    monkeypatch.delenv("PYJOBKIT_DSN", raising=False)
    monkeypatch.chdir(tmp_path)
    db = tmp_path / "async.db"
    dsn = f"sqlite+aiosqlite:///{db}"

    captured: dict[str, str] = {}

    real_config = cli_migrate._alembic_config

    def spy(sync_dsn: str):
        captured["dsn"] = sync_dsn
        return real_config(sync_dsn)

    monkeypatch.setattr(cli_migrate, "_alembic_config", spy)
    cli_migrate.main(["--dsn", dsn, "up"])
    assert captured["dsn"].startswith("sqlite:///")
