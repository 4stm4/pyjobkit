"""Built-in executor implementations."""

from .plugins import EXECUTOR_ENTRY_POINT_GROUP, discover_executors
from .subprocess import SubprocessExecutor
from .http import HttpExecutor

__all__ = [
    "SubprocessExecutor",
    "HttpExecutor",
    "discover_executors",
    "EXECUTOR_ENTRY_POINT_GROUP",
]


def __getattr__(name: str):  # type: ignore[no-untyped-def]
    """Lazy-import the optional DockerExecutor so missing aiodocker
    does not break the rest of the package."""

    if name == "DockerExecutor":
        from .docker import DockerExecutor as _DockerExecutor

        return _DockerExecutor
    raise AttributeError(name)
