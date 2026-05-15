"""Docker executor (#49) - run jobs inside an ephemeral container.

This executor takes a payload describing a container (image / command /
env / timeout) and runs it via :mod:`aiodocker`. ``aiodocker`` is an
optional dependency; install ``pyjobkit[docker]`` (or ``pip install
aiodocker``) to enable this executor.

Payload schema:

.. code-block:: python

    {
        "image": "alpine:3.20",           # required, str
        "command": ["echo", "hello"],     # optional, list[str] | str
        "env": {"KEY": "value"},          # optional, dict[str, str]
        "timeout_s": 60,                  # optional, defaults to 600
        "pull": True,                     # optional, pull image first
    }

The result mapping contains ``exit_code``, ``stdout``, and ``stderr``.
Non-zero exit codes raise a :class:`DockerExecutionError` so the
worker's retry / fail logic kicks in.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from uuid import UUID

from ..contracts import ExecContext, Executor

__all__ = ["DockerExecutor", "DockerExecutionError", "DockerDependencyMissing"]

logger = logging.getLogger(__name__)


class DockerDependencyMissing(RuntimeError):
    """Raised when aiodocker is not installed."""


class DockerExecutionError(RuntimeError):
    """Raised when the container exited with a non-zero status."""

    def __init__(self, exit_code: int, stdout: str, stderr: str) -> None:
        super().__init__(
            f"container exited with status {exit_code}: {stderr or stdout!r}"
        )
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr


def _require_aiodocker():  # type: ignore[no-untyped-def]
    try:
        import aiodocker  # type: ignore[import-not-found]
    except ImportError as exc:
        raise DockerDependencyMissing(
            "DockerExecutor requires the 'aiodocker' package; "
            "install pyjobkit[docker]."
        ) from exc
    return aiodocker


class DockerExecutor(Executor):
    """Run a one-shot Docker container per job."""

    kind = "docker"

    def __init__(
        self,
        *,
        default_timeout_s: int = 600,
        default_image: str | None = None,
    ) -> None:
        self.default_timeout_s = default_timeout_s
        self.default_image = default_image

    async def run(
        self, *, job_id: UUID, payload: dict, ctx: ExecContext
    ) -> dict:
        image = payload.get("image") or self.default_image
        if not image:
            raise ValueError("DockerExecutor payload must define 'image'")
        aiodocker = _require_aiodocker()
        command: list[str] | str | None = payload.get("command")
        env_map: dict[str, str] = dict(payload.get("env") or {})
        timeout_s: int = int(payload.get("timeout_s") or self.default_timeout_s)
        pull: bool = bool(payload.get("pull", True))

        config: dict[str, Any] = {
            "Image": image,
            "Env": [f"{k}={v}" for k, v in env_map.items()],
            "AttachStdout": True,
            "AttachStderr": True,
            "Tty": False,
        }
        if command is not None:
            config["Cmd"] = command if isinstance(command, list) else [command]

        client = aiodocker.Docker()
        try:
            if pull:
                await ctx.log(f"pulling image {image}")
                await client.images.pull(image)
            container = await client.containers.run(config=config)
            try:
                await ctx.log(f"started container {container.id[:12]}")
                try:
                    await asyncio.wait_for(container.wait(), timeout=timeout_s)
                except asyncio.TimeoutError:  # pragma: no cover - integration-test path
                    await ctx.log("container timed out; killing", stream="stderr")
                    await container.kill()
                    raise

                logs = await container.log(stdout=True, stderr=True)
                # aiodocker returns a list of strings; join into one blob.
                stdout = "".join(
                    line for line in await container.log(stdout=True, stderr=False)
                )
                stderr = "".join(
                    line for line in await container.log(stdout=False, stderr=True)
                )

                state = await container.show()
                exit_code = int(state.get("State", {}).get("ExitCode", -1))
                if exit_code != 0:
                    raise DockerExecutionError(exit_code, stdout, stderr)

                return {
                    "exit_code": exit_code,
                    "stdout": stdout,
                    "stderr": stderr,
                    "container_id": container.id,
                }
            finally:
                try:
                    await container.delete(force=True)
                except Exception as exc:  # pragma: no cover - cleanup
                    logger.warning("failed to delete container: %s", exc)
        finally:
            await client.close()
