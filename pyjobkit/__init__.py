"""Pyjobkit: backend-agnostic job processing toolkit."""

from .backends.memory import MemoryBackend
from .config import Config, ConfigError, load_config
from .engine import Engine
from .worker import Worker
from .contracts import ExecContext, Executor, QueueBackend
from .retry import (
    ExponentialBackoff,
    FixedDelay,
    JitteredExponentialBackoff,
    RetryPolicy,
    parse_policy,
)
from .types import FailureReason, JobRecord, JobResult, JobStatus, LogStream

__version__ = "0.2.0"

__all__ = [
    "Engine",
    "Worker",
    "Executor",
    "QueueBackend",
    "ExecContext",
    "MemoryBackend",
    "Config",
    "ConfigError",
    "load_config",
    "FailureReason",
    "JobRecord",
    "JobResult",
    "JobStatus",
    "LogStream",
    "RetryPolicy",
    "FixedDelay",
    "ExponentialBackoff",
    "JitteredExponentialBackoff",
    "parse_policy",
    "__version__",
]
