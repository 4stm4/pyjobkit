"""Configuration loading from TOML files and environment variables.

Resolution order (highest priority wins):

1. Explicit overrides (e.g. CLI flags).
2. Environment variables prefixed with ``PYJOBKIT_``.
3. ``[pyjobkit]`` table in a TOML file (default ``.pyjobkit.toml`` in CWD,
   or the path passed via ``config_path``).
4. Built-in defaults.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any, Mapping

DEFAULT_CONFIG_FILENAME = ".pyjobkit.toml"
ENV_PREFIX = "PYJOBKIT_"

_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}

LOG_LEVELS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")
LOG_FORMATS = ("text", "json")


class ConfigError(ValueError):
    """Raised when configuration values are invalid or cannot be loaded."""


@dataclass
class Config:
    """Resolved Pyjobkit configuration."""

    dsn: str | None = None
    poll_interval: float = 0.5
    max_attempts: int = 3
    default_executor: str | None = None
    concurrency: int = 8
    batch: int = 1
    lease_ttl: int = 30
    log_level: str = "INFO"
    log_format: str = "text"
    retry_policy: str = "exponential:1:2"
    watchdog_interval_s: float | None = None
    disable_skip_locked: bool = False
    enable_plugins: bool = False
    extra_executors: tuple[str, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


# Aliases accepted in TOML/env (issue #46 calls the DSN ``db_url``).
_KEY_ALIASES = {
    "db_url": "dsn",
    "database_url": "dsn",
}


def _coerce_bool(value: Any, key: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in _TRUTHY:
            return True
        if lowered in _FALSY:
            return False
    raise ConfigError(f"{key!r} must be a boolean (got {value!r})")


def _coerce_int(value: Any, key: str, *, positive: bool = True) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{key!r} must be an integer (got {value!r})") from exc
    if positive and result <= 0:
        raise ConfigError(f"{key!r} must be > 0 (got {result})")
    return result


def _coerce_float(value: Any, key: str, *, positive: bool = True) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{key!r} must be a number (got {value!r})") from exc
    if positive and result <= 0:
        raise ConfigError(f"{key!r} must be > 0 (got {result})")
    return result


def _coerce_log_level(value: Any) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"'log_level' must be a string (got {value!r})")
    upper = value.strip().upper()
    if upper not in LOG_LEVELS:
        raise ConfigError(
            f"'log_level' must be one of {LOG_LEVELS} (got {value!r})"
        )
    return upper


def _coerce_log_format(value: Any) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"'log_format' must be a string (got {value!r})")
    lower = value.strip().lower()
    if lower not in LOG_FORMATS:
        raise ConfigError(
            f"'log_format' must be one of {LOG_FORMATS} (got {value!r})"
        )
    return lower


def _coerce_executors(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        items = [s.strip() for s in value.split(",")]
    elif isinstance(value, (list, tuple)):
        items = [str(s).strip() for s in value]
    else:
        raise ConfigError(
            f"'extra_executors' must be a string or list (got {value!r})"
        )
    return tuple(item for item in items if item)


_FIELD_NAMES = {f.name for f in fields(Config)}


def _normalize_key(key: str) -> str | None:
    canonical = _KEY_ALIASES.get(key, key)
    return canonical if canonical in _FIELD_NAMES else None


def _coerce_value(key: str, value: Any) -> Any:
    if key == "poll_interval":
        return _coerce_float(value, key)
    if key == "watchdog_interval_s":
        if value is None or value == "":
            return None
        return _coerce_float(value, key)
    if key in {"max_attempts", "concurrency", "batch", "lease_ttl"}:
        return _coerce_int(value, key)
    if key in {"disable_skip_locked", "enable_plugins"}:
        return _coerce_bool(value, key)
    if key == "log_level":
        return _coerce_log_level(value)
    if key == "log_format":
        return _coerce_log_format(value)
    if key == "extra_executors":
        return _coerce_executors(value)
    if key == "retry_policy":
        if not isinstance(value, str) or not value.strip():
            raise ConfigError(f"'retry_policy' must be a non-empty string (got {value!r})")
        return value.strip()
    if key in {"dsn", "default_executor"}:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ConfigError(f"{key!r} must be a string (got {value!r})")
        return value
    raise ConfigError(f"Unknown config key: {key!r}")


def _layer_from_mapping(source: Mapping[str, Any], *, source_label: str) -> dict[str, Any]:
    layer: dict[str, Any] = {}
    for raw_key, value in source.items():
        key = _normalize_key(raw_key)
        if key is None:
            # Silently ignore unknown keys to allow forward-compat additions.
            continue
        try:
            layer[key] = _coerce_value(key, value)
        except ConfigError as exc:
            raise ConfigError(f"{source_label}: {exc}") from exc
    return layer


def _read_toml(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as fh:
            data = tomllib.load(fh)
    except OSError as exc:
        raise ConfigError(f"Cannot read config file {path}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Invalid TOML in {path}: {exc}") from exc
    section = data.get("pyjobkit", data)
    if not isinstance(section, dict):
        raise ConfigError(f"{path}: '[pyjobkit]' section must be a table")
    return section


def _layer_from_env(env: Mapping[str, str]) -> dict[str, Any]:
    raw: dict[str, Any] = {}
    for full_key, value in env.items():
        if not full_key.startswith(ENV_PREFIX):
            continue
        raw_key = full_key[len(ENV_PREFIX):].lower()
        raw[raw_key] = value
    return _layer_from_mapping(raw, source_label="environment")


def find_config_file(start: Path | None = None) -> Path | None:
    """Return the path to ``.pyjobkit.toml`` in ``start`` (default CWD), if present."""

    base = start or Path.cwd()
    candidate = base / DEFAULT_CONFIG_FILENAME
    return candidate if candidate.is_file() else None


def load_config(
    *,
    config_path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    overrides: Mapping[str, Any] | None = None,
    search_cwd: bool = True,
) -> Config:
    """Load and merge configuration from TOML, environment, and overrides."""

    layers: list[dict[str, Any]] = []

    path: Path | None = None
    if config_path is not None:
        path = Path(config_path)
        if not path.is_file():
            raise ConfigError(f"Config file not found: {path}")
    elif search_cwd:
        path = find_config_file()

    if path is not None:
        layers.append(_layer_from_mapping(_read_toml(path), source_label=str(path)))

    layers.append(_layer_from_env(env if env is not None else os.environ))

    if overrides:
        layers.append(_layer_from_mapping(overrides, source_label="overrides"))

    merged: dict[str, Any] = {}
    for layer in layers:
        merged.update(layer)

    return Config(**merged)
