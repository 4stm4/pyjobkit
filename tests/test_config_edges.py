"""Edge-case coverage for pyjobkit.config coercion / parsing helpers."""

from __future__ import annotations

import pytest

from pyjobkit.config import (
    Config,
    ConfigError,
    _coerce_bool,
    _coerce_executors,
    _coerce_float,
    _coerce_int,
    _coerce_log_format,
    _coerce_log_level,
    _coerce_value,
    _parse_rate_limits_string,
)


def test_config_as_dict_roundtrips() -> None:
    cfg = Config(dsn="sqlite://", concurrency=4)
    out = cfg.as_dict()
    assert out["dsn"] == "sqlite://"
    assert out["concurrency"] == 4


@pytest.mark.parametrize("raw", ["yes", "1", "TRUE", "on"])
def test_coerce_bool_truthy_strings(raw: str) -> None:
    assert _coerce_bool(raw, "x") is True


@pytest.mark.parametrize("raw", ["no", "0", "FALSE", "off"])
def test_coerce_bool_falsy_strings(raw: str) -> None:
    assert _coerce_bool(raw, "x") is False


def test_coerce_bool_rejects_other() -> None:
    with pytest.raises(ConfigError):
        _coerce_bool(42, "x")


def test_coerce_int_rejects_non_numeric() -> None:
    with pytest.raises(ConfigError):
        _coerce_int("abc", "x")


def test_coerce_int_rejects_non_positive() -> None:
    with pytest.raises(ConfigError):
        _coerce_int(0, "x")


def test_coerce_int_accepts_when_positive_false() -> None:
    assert _coerce_int(0, "x", positive=False) == 0


def test_coerce_float_rejects() -> None:
    with pytest.raises(ConfigError):
        _coerce_float("abc", "x")
    with pytest.raises(ConfigError):
        _coerce_float(-1, "x")


def test_coerce_log_level_rejects_non_string() -> None:
    with pytest.raises(ConfigError):
        _coerce_log_level(42)


def test_coerce_log_format_rejects_non_string() -> None:
    with pytest.raises(ConfigError):
        _coerce_log_format(42)


def test_coerce_executors_handles_all_input_shapes() -> None:
    assert _coerce_executors(None) == ()
    assert _coerce_executors(["a:b", "c:d"]) == ("a:b", "c:d")
    assert _coerce_executors("a:b, c:d") == ("a:b", "c:d")
    with pytest.raises(ConfigError):
        _coerce_executors({"unsupported": "shape"})


def test_parse_rate_limits_string_with_explicit_burst() -> None:
    parsed = _parse_rate_limits_string("http:5:10")
    assert parsed == {"http": {"max_per_second": 5.0, "burst": 10.0}}


def test_parse_rate_limits_string_rejects_garbage() -> None:
    with pytest.raises(ConfigError):
        _parse_rate_limits_string("http:abc")
    with pytest.raises(ConfigError):
        _parse_rate_limits_string("http")
    with pytest.raises(ConfigError):
        _parse_rate_limits_string(":5")


def test_coerce_value_watchdog_interval_blank_and_zero_strings() -> None:
    assert _coerce_value("watchdog_interval_s", None) is None
    assert _coerce_value("watchdog_interval_s", "") is None
    assert _coerce_value("watchdog_interval_s", "1.5") == 1.5


def test_coerce_value_retry_policy_rejects_blank() -> None:
    with pytest.raises(ConfigError):
        _coerce_value("retry_policy", "")
    with pytest.raises(ConfigError):
        _coerce_value("retry_policy", 42)


def test_coerce_value_dsn_validation() -> None:
    assert _coerce_value("dsn", None) is None
    with pytest.raises(ConfigError):
        _coerce_value("dsn", 42)


def test_coerce_value_rate_limits_accepts_mapping_and_rejects_garbage() -> None:
    assert _coerce_value("rate_limits", {"http": {"max_per_second": 1}}) == {
        "http": {"max_per_second": 1}
    }
    assert _coerce_value("rate_limits", "http:1:2") == {
        "http": {"max_per_second": 1.0, "burst": 2.0}
    }
    with pytest.raises(ConfigError):
        _coerce_value("rate_limits", 42)


def test_coerce_value_unknown_key_raises() -> None:
    with pytest.raises(ConfigError):
        _coerce_value("nope", 1)
