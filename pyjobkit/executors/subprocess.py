"""Subprocess executor streaming stdout/stderr into the log sink."""

from __future__ import annotations

import asyncio
import time
from contextlib import suppress
from typing import Any
from uuid import UUID

from ..contracts import ExecContext, Executor


class SubprocessExecutor(Executor):
    kind = "subprocess"

    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext) -> dict:
        cmd = payload["cmd"]
        env = payload.get("env")
        cwd = payload.get("cwd")
        shell = isinstance(cmd, str)
        start = time.perf_counter()
        proc: asyncio.subprocess.Process | None = None
        pump_tasks: list[asyncio.Task[None]] = []
        cancelled = False
        logged_return_code = False

        async def _safe_log(message: str, /, *, stream: str = "stdout") -> None:
            with suppress(Exception):
                await ctx.log(message, stream=stream)

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
                    try:
                        chunk = await asyncio.wait_for(stream.readline(), timeout=1)
                    except asyncio.TimeoutError:
                        if proc and proc.returncode is None:
                            continue
                        break
                    if not chunk:
                        break
                    await _safe_log(chunk.decode(errors="ignore"), stream=name)

            if proc.stdout is not None:
                pump_tasks.append(asyncio.create_task(_pump(proc.stdout, "stdout")))
            if proc.stderr is not None:
                pump_tasks.append(asyncio.create_task(_pump(proc.stderr, "stderr")))
            rc = await proc.wait()
            if rc != 0:
                await _safe_log(f"process exited with code {rc}", stream="stderr")
                logged_return_code = True
            for t in pump_tasks:
                t.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*pump_tasks)
            return {
                "returncode": rc,
                "duration_ms": int((time.perf_counter() - start) * 1000),
            }
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if not cancelled:
                current = asyncio.current_task()
                cancelled = bool(current and current.cancelled())
            cleanup_cancelled = False
            if proc and proc.returncode is None:
                reason = "cancelled" if cancelled else "error"
                await _safe_log(f"terminating process ({reason})", stream="stderr")
                proc.terminate()
                try:
                    await asyncio.wait_for(asyncio.shield(proc.wait()), timeout=3)
                except asyncio.TimeoutError:
                    await _safe_log(
                        "process unresponsive to SIGTERM; sending SIGKILL", stream="stderr"
                    )
                    proc.kill()
                    with suppress(asyncio.CancelledError):
                        await proc.wait()
                except asyncio.CancelledError:
                    cleanup_cancelled = True
                    proc.kill()
                    with suppress(asyncio.CancelledError):
                        await proc.wait()
            for t in pump_tasks:
                if not t.done():
                    t.cancel()
            if pump_tasks:
                with suppress(asyncio.CancelledError):
                    await asyncio.gather(*pump_tasks)
            if (
                proc
                and proc.returncode is not None
                and proc.returncode != 0
                and not logged_return_code
            ):
                await _safe_log(
                    f"process exited with code {proc.returncode}", stream="stderr"
                )
            if cleanup_cancelled:
                raise asyncio.CancelledError
