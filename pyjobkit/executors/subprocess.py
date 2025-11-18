"""Subprocess executor streaming stdout/stderr into the log sink."""

from __future__ import annotations

import asyncio
import time
from contextlib import suppress
from typing import Any

from ..contracts import ExecContext


class SubprocessExecutor:
    kind = "subprocess"

    async def run(self, *, job_id, payload: dict, ctx: ExecContext) -> dict:  # type: ignore[override]
        cmd = payload["cmd"]
        env = payload.get("env")
        cwd = payload.get("cwd")
        shell = isinstance(cmd, str)
        start = time.perf_counter()
        proc: asyncio.subprocess.Process | None = None
        try:
            if shell:
                proc = await asyncio.create_subprocess_shell(
                    cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=cwd,
                    env=env,
                )
            else:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=cwd,
                    env=env,
                )

            async def _pump(stream: asyncio.StreamReader | None, name: str) -> None:
                if stream is None:
                    return
                while True:
                    chunk = await stream.readline()
                    if not chunk:
                        break
                    await ctx.log(chunk.decode(errors="ignore"), stream=name)

            async with asyncio.TaskGroup() as tg:
                tg.create_task(_pump(proc.stdout, "stdout"))
                tg.create_task(_pump(proc.stderr, "stderr"))
            rc = await proc.wait()
            return {
                "returncode": rc,
                "duration_ms": int((time.perf_counter() - start) * 1000),
            }
        except asyncio.CancelledError:
            if proc and proc.returncode is None:
                proc.kill()
                with suppress(asyncio.CancelledError):
                    await proc.wait()
            raise
        except Exception:
            if proc and proc.returncode is None:
                proc.kill()
                with suppress(asyncio.CancelledError):
                    await proc.wait()
            raise
