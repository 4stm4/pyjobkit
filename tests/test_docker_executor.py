"""Unit tests for the DockerExecutor (#49).

The tests fake the ``aiodocker`` module so they never touch a real
Docker daemon. They exercise the wiring: payload validation, image
pull, container life cycle, log capture, non-zero exit handling, and
the optional-dependency error path.
"""

from __future__ import annotations

import asyncio
import sys
import types
from uuid import uuid4

import pytest

from pyjobkit.contracts import ExecContext
from pyjobkit.executors.docker import (
    DockerDependencyMissing,
    DockerExecutionError,
    DockerExecutor,
)


class _Ctx(ExecContext):
    def __init__(self) -> None:
        self.lines: list[tuple[str, str]] = []

    async def log(self, message: str, /, *, stream: str = "stdout") -> None:
        self.lines.append((stream, message))

    async def is_cancelled(self) -> bool:  # pragma: no cover
        return False

    async def set_progress(self, value: float, /, **meta) -> None:  # pragma: no cover
        pass


class _FakeContainer:
    def __init__(self, exit_code: int = 0, stdout: str = "ok\n", stderr: str = "") -> None:
        self.id = "abc1234567890"
        self._exit = exit_code
        self._stdout = stdout
        self._stderr = stderr
        self.deleted = False
        self.killed = False

    async def wait(self) -> None:
        return None

    async def log(self, *, stdout: bool, stderr: bool):
        out: list[str] = []
        if stdout:
            out.append(self._stdout)
        if stderr:
            out.append(self._stderr)
        return out

    async def show(self) -> dict:
        return {"State": {"ExitCode": self._exit}}

    async def kill(self) -> None:
        self.killed = True

    async def delete(self, *, force: bool = False) -> None:
        self.deleted = True


class _FakeContainers:
    def __init__(self, container: _FakeContainer) -> None:
        self._container = container

    async def run(self, *, config: dict) -> _FakeContainer:
        self._container.config = config  # type: ignore[attr-defined]
        return self._container


class _FakeImages:
    def __init__(self) -> None:
        self.pulled: list[str] = []

    async def pull(self, image: str) -> None:
        self.pulled.append(image)


class _FakeDocker:
    def __init__(self, container: _FakeContainer | None = None) -> None:
        self.images = _FakeImages()
        self.containers = _FakeContainers(container or _FakeContainer())
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _install_fake_aiodocker(
    monkeypatch: pytest.MonkeyPatch, container: _FakeContainer
) -> _FakeDocker:
    fake_docker = _FakeDocker(container)

    class _FakeModule(types.ModuleType):
        Docker = staticmethod(lambda: fake_docker)

    module = _FakeModule("aiodocker")
    monkeypatch.setitem(sys.modules, "aiodocker", module)
    return fake_docker


def test_run_success_returns_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _run() -> None:
        fake = _install_fake_aiodocker(monkeypatch, _FakeContainer(0, "hi\n", ""))
        executor = DockerExecutor()
        result = await executor.run(
            job_id=uuid4(),
            payload={
                "image": "alpine:3.20",
                "command": ["echo", "hi"],
                "env": {"FOO": "bar"},
            },
            ctx=_Ctx(),
        )
        assert result["exit_code"] == 0
        assert "hi" in result["stdout"]
        assert fake.images.pulled == ["alpine:3.20"]
        assert fake.containers._container.config["Cmd"] == ["echo", "hi"]
        assert fake.containers._container.deleted is True
        assert fake.closed is True

    asyncio.run(_run())


def test_run_failure_raises_docker_execution_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _run() -> None:
        _install_fake_aiodocker(monkeypatch, _FakeContainer(2, "", "boom"))
        executor = DockerExecutor()
        with pytest.raises(DockerExecutionError) as excinfo:
            await executor.run(
                job_id=uuid4(),
                payload={"image": "alpine"},
                ctx=_Ctx(),
            )
        assert excinfo.value.exit_code == 2

    asyncio.run(_run())


def test_run_requires_image() -> None:
    async def _run() -> None:
        executor = DockerExecutor()
        with pytest.raises(ValueError):
            await executor.run(job_id=uuid4(), payload={}, ctx=_Ctx())

    asyncio.run(_run())


def test_missing_aiodocker_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _run() -> None:
        # Pretend aiodocker is uninstalled.
        monkeypatch.setitem(sys.modules, "aiodocker", None)  # type: ignore[arg-type]
        executor = DockerExecutor()
        with pytest.raises(DockerDependencyMissing):
            await executor.run(
                job_id=uuid4(),
                payload={"image": "alpine"},
                ctx=_Ctx(),
            )

    asyncio.run(_run())
