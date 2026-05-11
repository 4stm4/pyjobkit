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
