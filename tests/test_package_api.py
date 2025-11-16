"""Simple tests verifying public exports are wired correctly."""

from pyjobkit import Engine, ExecContext, Executor, QueueBackend, Worker
from pyjobkit.backends import MemoryBackend, SQLBackend
from pyjobkit.events import LocalEventBus
from pyjobkit.logging import MemoryLogSink


def test_public_api_exports() -> None:
    assert Engine.__module__ == "pyjobkit.engine"
    assert Worker.__module__ == "pyjobkit.worker"
    assert Executor.__module__ == "pyjobkit.contracts"
    assert QueueBackend.__module__ == "pyjobkit.contracts"
    assert ExecContext.__module__ == "pyjobkit.contracts"
    assert MemoryBackend.__module__ == "pyjobkit.backends.memory"
    assert SQLBackend.__module__ == "pyjobkit.backends.sql.backend"
    assert LocalEventBus.__module__ == "pyjobkit.events.local"
    assert MemoryLogSink.__module__ == "pyjobkit.logging.memory"
