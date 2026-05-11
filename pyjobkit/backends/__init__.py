"""Backend implementations."""

from .memory import MemoryBackend
from .sql.backend import SQLBackend

__all__ = ["MemoryBackend", "SQLBackend"]


def __getattr__(name: str):  # type: ignore[no-untyped-def]
    """Lazy-import RedisBackend so missing `redis` doesn't break import."""

    if name == "RedisBackend":
        from .redis import RedisBackend as _RedisBackend

        return _RedisBackend
    raise AttributeError(name)
