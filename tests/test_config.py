"""Tests for ``pyjobkit.config``."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from pyjobkit.config import (
    Config,
    ConfigError,
    find_config_file,
    load_config,
)


def _write_toml(tmp_path: Path, body: str) -> Path:
    path = tmp_path / ".pyjobkit.toml"
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return path


def test_defaults_when_no_sources(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    cfg = load_config(env={})
    assert cfg == Config()


def test_loads_from_toml(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [pyjobkit]
        dsn = "sqlite+aiosqlite:///jobs.db"
        poll_interval = 1.5
        max_attempts = 7
        default_executor = "myapp.exec:make"
        concurrency = 16
        batch = 4
        lease_ttl = 60
        log_level = "debug"
        disable_skip_locked = true
        extra_executors = ["a:b", "c:d"]
        """,
    )
    cfg = load_config(config_path=path, env={})
    assert cfg.dsn == "sqlite+aiosqlite:///jobs.db"
    assert cfg.poll_interval == 1.5
    assert cfg.max_attempts == 7
    assert cfg.default_executor == "myapp.exec:make"
    assert cfg.concurrency == 16
    assert cfg.batch == 4
    assert cfg.lease_ttl == 60
    assert cfg.log_level == "DEBUG"
    assert cfg.disable_skip_locked is True
    assert cfg.extra_executors == ("a:b", "c:d")


def test_db_url_alias(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [pyjobkit]
        db_url = "postgresql+asyncpg://localhost/db"
        """,
    )
    cfg = load_config(config_path=path, env={})
    assert cfg.dsn == "postgresql+asyncpg://localhost/db"


def test_env_overrides_toml(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [pyjobkit]
        dsn = "sqlite://from-toml"
        poll_interval = 0.25
        """,
    )
    env = {
        "PYJOBKIT_DSN": "sqlite://from-env",
        "PYJOBKIT_POLL_INTERVAL": "2.0",
        "PYJOBKIT_DISABLE_SKIP_LOCKED": "true",
        "PYJOBKIT_MAX_ATTEMPTS": "9",
    }
    cfg = load_config(config_path=path, env=env)
    assert cfg.dsn == "sqlite://from-env"
    assert cfg.poll_interval == 2.0
    assert cfg.disable_skip_locked is True
    assert cfg.max_attempts == 9


def test_overrides_win_over_env_and_toml(tmp_path: Path) -> None:
    path = _write_toml(tmp_path, '[pyjobkit]\ndsn = "from-toml"\n')
    env = {"PYJOBKIT_DSN": "from-env"}
    cfg = load_config(config_path=path, env=env, overrides={"dsn": "from-cli"})
    assert cfg.dsn == "from-cli"


def test_extra_executors_from_env_csv() -> None:
    cfg = load_config(env={"PYJOBKIT_EXTRA_EXECUTORS": "x:y, z:w , "})
    assert cfg.extra_executors == ("x:y", "z:w")


def test_unknown_keys_are_ignored(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [pyjobkit]
        dsn = "sqlite://"
        unknown_field = 42
        """,
    )
    cfg = load_config(config_path=path, env={})
    assert cfg.dsn == "sqlite://"


def test_invalid_log_level(tmp_path: Path) -> None:
    path = _write_toml(tmp_path, '[pyjobkit]\nlog_level = "verbose"\n')
    with pytest.raises(ConfigError, match="log_level"):
        load_config(config_path=path, env={})


def test_invalid_int(tmp_path: Path) -> None:
    path = _write_toml(tmp_path, "[pyjobkit]\nconcurrency = 0\n")
    with pytest.raises(ConfigError, match="concurrency"):
        load_config(config_path=path, env={})


def test_invalid_bool_in_env() -> None:
    with pytest.raises(ConfigError, match="disable_skip_locked"):
        load_config(env={"PYJOBKIT_DISABLE_SKIP_LOCKED": "maybe"})


def test_missing_explicit_config_path(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config(config_path=tmp_path / "nope.toml", env={})


def test_invalid_toml_syntax(tmp_path: Path) -> None:
    path = tmp_path / ".pyjobkit.toml"
    path.write_text("not = valid = toml", encoding="utf-8")
    with pytest.raises(ConfigError, match="Invalid TOML"):
        load_config(config_path=path, env={})


def test_find_config_file(tmp_path: Path) -> None:
    assert find_config_file(tmp_path) is None
    (tmp_path / ".pyjobkit.toml").write_text("", encoding="utf-8")
    assert find_config_file(tmp_path) == tmp_path / ".pyjobkit.toml"


def test_search_cwd_picks_up_default(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_toml(tmp_path, '[pyjobkit]\ndsn = "from-cwd"\n')
    monkeypatch.chdir(tmp_path)
    cfg = load_config(env={})
    assert cfg.dsn == "from-cwd"


def test_search_cwd_disabled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_toml(tmp_path, '[pyjobkit]\ndsn = "from-cwd"\n')
    monkeypatch.chdir(tmp_path)
    cfg = load_config(env={}, search_cwd=False)
    assert cfg.dsn is None


def test_toml_without_section(tmp_path: Path) -> None:
    path = tmp_path / ".pyjobkit.toml"
    path.write_text('dsn = "top-level"\n', encoding="utf-8")
    cfg = load_config(config_path=path, env={})
    assert cfg.dsn == "top-level"
