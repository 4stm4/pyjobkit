"""Simple tests verifying public exports are wired correctly."""

from jobkit import Engine, ExecContext, Executor, QueueBackend, Worker
from jobkit.backends import MemoryBackend, SQLBackend
from jobkit.events import LocalEventBus
from jobkit.logging import MemoryLogSink


def test_public_api_exports() -> None:
    assert Engine.__module__ == "jobkit.engine"
    assert Worker.__module__ == "jobkit.worker"
    assert Executor.__module__ == "jobkit.contracts"
    assert QueueBackend.__module__ == "jobkit.contracts"
    assert ExecContext.__module__ == "jobkit.contracts"
    assert MemoryBackend.__module__ == "jobkit.backends.memory"
    assert SQLBackend.__module__ == "jobkit.backends.sql.backend"
    assert LocalEventBus.__module__ == "jobkit.events.local"
    assert MemoryLogSink.__module__ == "jobkit.logging.memory"
