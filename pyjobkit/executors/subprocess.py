"""Subprocess executor streaming stdout/stderr into the log sink.

.. warning::

   The executor runs commands taken directly from the job payload. If
   ``enqueue`` is exposed to untrusted callers (for example via the REST
   integration without authentication) attackers can run arbitrary
   commands. Either keep the producer surface trusted, or construct the
   executor with an ``allowed_commands`` allowlist so only specific
   programs may be launched.
"""

from __future__ import annotations

import asyncio
import logging
import shlex
import time
from contextlib import suppress
from typing import Any, Iterable
from uuid import UUID

from ..contracts import ExecContext, Executor

logger = logging.getLogger(__name__)


class SubprocessExecutor(Executor):
    kind = "subprocess"

    def __init__(
        self,
        *,
        allowed_commands: Iterable[str] | None = None,
    ) -> None:
        """Initialise the executor.

        Parameters:
            allowed_commands: When set, only payload commands whose
                first token (program name, taken from ``argv[0]`` or
                the leftmost word of a shell string) matches one of
                these strings will be launched. Anything else raises
                ``PermissionError``. ``None`` (the default) allows
                anything but logs a warning at first run.
        """

        self._allowed = (
            frozenset(allowed_commands) if allowed_commands is not None else None
        )
        self._warned_unrestricted = False

    def _check_allowed(self, cmd: str | list[str]) -> str:
        is_shell_string = isinstance(cmd, str)
        if is_shell_string:
            parts = shlex.split(cmd)
            head = parts[0] if parts else ""
        else:
            head = cmd[0] if cmd else ""
        if self._allowed is None:
            if not self._warned_unrestricted:
                logger.warning(
                    "SubprocessExecutor running without allowed_commands; "
                    "do not expose enqueue to untrusted callers."
                )
                self._warned_unrestricted = True
            return head
        # When an allowlist is configured we *must not* run shell strings:
        # even with a permitted head like "bash" or "sh", the rest of the
        # string is interpreted by the shell and trivially bypasses the
        # filter (e.g. "bash -c 'rm -rf /'"). Force the producer to pass
        # an argv list so the allowlist's first-token check is meaningful.
        if is_shell_string:
            raise PermissionError(
                "subprocess command must be a list when allowed_commands is set; "
                "shell strings bypass the allowlist"
            )
        if head not in self._allowed:
            raise PermissionError(
                f"subprocess command {head!r} is not in the allowlist"
            )
        return head

    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext) -> dict:
        cmd = payload["cmd"]
        env = payload.get("env")
        cwd = payload.get("cwd")
        shell = isinstance(cmd, str)
        self._check_allowed(cmd)
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
