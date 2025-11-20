
##  Pyjobkit 0.2.0 – Stable Production Release

###  Major Improvements

* **Reliable Job Cancellation**
  Jobs cancelled during execution (`Engine.cancel()`) no longer reach the `succeeded` state. Cancellation is now enforced consistently.

* **Timeout Handling Respects max_attempts**
  Jobs that exceed `timeout_s` are now retried (if `max_attempts` allows), using exponential backoff. Previously, such jobs were marked as permanently failed.

* **Graceful Worker Shutdown**
  `Worker.wait_stopped()` now properly signals after all tasks are drained, enabling clean process termination.

* **Subprocess Cleanup on Cancel or Timeout**
  `SubprocessExecutor` now terminates or force-kills subprocesses if they outlive their job timeout or are cancelled, preventing zombie processes.

### Internal Fixes

* Executors now correctly propagate `CancelledError`, avoiding retries after cancellation.
* Worker loop is more robust to backend failures and transient errors.
* Improved fallback behavior for queueing backends without `SKIP LOCKED`.

---
