"""Tests for entry-point based executor discovery (#51)."""

from __future__ import annotations

import asyncio
import importlib.metadata
import logging
from typing import Any
from uuid import UUID

import pytest

from pyjobkit import MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.engine import Engine
from pyjobkit.executors.plugins import (
    EXECUTOR_ENTRY_POINT_GROUP,
    discover_executors,
)


class _PluginExecutor(Executor):
    kind = "plugin-kind"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return {"ok": True}


def _make_plugin() -> Executor:
    return _PluginExecutor()


def _bad_factory() -> Any:
    return "not an executor"


def _raising_factory() -> Any:
    raise RuntimeError("boom")


class _FakeEntryPoint:
    def __init__(self, name: str, target):
        self.name = name
        self._target = target

    def load(self):
        if isinstance(self._target, type) or callable(self._target):
            return self._target
        raise ImportError(self._target)


def _install_entry_points(monkeypatch: pytest.MonkeyPatch, entries: list[_FakeEntryPoint]) -> None:
    def fake(*, group: str):
        assert group == EXECUTOR_ENTRY_POINT_GROUP
        return entries

    monkeypatch.setattr(importlib.metadata, "entry_points", fake)


def test_discover_returns_loaded_executors(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_entry_points(
        monkeypatch, [_FakeEntryPoint("plugin", _make_plugin)]
    )
    result = discover_executors()
    assert len(result) == 1
    assert isinstance(result[0], _PluginExecutor)


def test_discover_filters_by_only(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_entry_points(
        monkeypatch,
        [
            _FakeEntryPoint("plugin", _make_plugin),
            _FakeEntryPoint("other", _make_plugin),
        ],
    )
    result = discover_executors(only=["plugin"])
    assert len(result) == 1


def test_discover_skips_factory_that_returns_non_executor(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _install_entry_points(
        monkeypatch, [_FakeEntryPoint("broken", _bad_factory)]
    )
    with caplog.at_level(logging.WARNING):
        result = discover_executors()
    assert result == []
    assert any("did not return an Executor" in r.message for r in caplog.records)


def test_discover_skips_factory_that_raises(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _install_entry_points(
        monkeypatch, [_FakeEntryPoint("kaboom", _raising_factory)]
    )
    with caplog.at_level(logging.WARNING):
        result = discover_executors()
    assert result == []
    assert any("raised during factory call" in r.message for r in caplog.records)


def test_engine_register_plugins_adds_to_executors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_entry_points(
        monkeypatch, [_FakeEntryPoint("plugin", _make_plugin)]
    )
    engine = Engine(backend=MemoryBackend(), executors=[])
    registered = engine.register_plugins()
    assert len(registered) == 1
    assert engine.executor_for("plugin-kind") is registered[0]


def test_engine_register_plugins_skips_duplicate(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    _install_entry_points(
        monkeypatch, [_FakeEntryPoint("plugin", _make_plugin)]
    )
    pre_existing = _PluginExecutor()
    engine = Engine(backend=MemoryBackend(), executors=[pre_existing])
    with caplog.at_level(logging.WARNING):
        registered = engine.register_plugins()
    assert registered == []
    assert engine.executor_for("plugin-kind") is pre_existing
    assert any("already registered" in r.message for r in caplog.records)
