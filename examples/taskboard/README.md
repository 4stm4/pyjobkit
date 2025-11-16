# Taskboard demo

This example spins up a FastAPI application plus an in-process Jobkit worker.  The
single-page UI lets you enqueue demo jobs that simply sleep for a random
interval and then complete.

## Running locally

1. Install Jobkit and the extra demo dependencies:

   ```bash
   pip install -e .
   pip install fastapi uvicorn
   ```

2. Start the web server (it also runs the worker loop in the background):

   ```bash
   uvicorn examples.taskboard.app:app --reload
   ```

Visit <http://127.0.0.1:8000/> and click **Enqueue demo job** to populate the
table.

## Docker

You can also run the demo in a container built from the repository root:

```bash
docker build -f examples/taskboard/Dockerfile -t jobkit-taskboard .
docker run --rm -p 8000:8000 jobkit-taskboard
```

The container uses the in-memory backend, so job history is reset whenever the
process restarts.
