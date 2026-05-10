"""Pyjobkit: backend-agnostic job processing toolkit."""

from .config import Config, ConfigError, load_config
from .engine import Engine
from .worker import Worker
from .contracts import ExecContext, Executor, QueueBackend

__version__ = "0.2.0"

__all__ = [
    "Engine",
    "Worker",
    "Executor",
    "QueueBackend",
    "ExecContext",
    "Config",
    "ConfigError",
    "load_config",
    "__version__",
]
