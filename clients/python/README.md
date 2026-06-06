# Foreman Python client

Drop-in HTTP client + optional Worker loop for Foreman. Two files,
`requests` is the only third-party dependency.

| File | What it gives you |
|---|---|
| [`foreman.py`](./foreman.py) | `Client` — raw HTTP wrapper for every API endpoint |
| [`worker.py`](./worker.py)   | `Worker` — claim → heartbeat → handle → terminate loop |

Drop just `foreman.py` if you only need to *kick off* jobs from a
producer service. Drop both if you also need to *run* them. `worker.py`
imports from `foreman.py`, so they need to live alongside each other.

## Install

```sh
pip install requests
# then copy the file(s) into your project, e.g. yourapp/foreman/
```

```
yourapp/foreman/
├── __init__.py
├── foreman.py
└── worker.py   # optional
```

Minimal `__init__.py`:

```python
from .foreman import Client, HTTPError, Job, Run, Schedule, EnqueueResult, HeartbeatResult
from .worker import Worker, Progress, FailError, PermanentError
```

## Client quick-start

```python
from yourapp.foreman import Client

c = Client("http://foreman.local:7011")

res = c.enqueue(
    kind="send-email",
    params={"to": "x@y.com"},
    idempotency_key="email-42",  # re-sends are no-ops
    max_attempts=3,
)
print(res.created, res.job.id)
```

The 409 idempotency-key collision is **not** raised — `enqueue` returns
`EnqueueResult(created=False, job=existing)` so re-sends are explicitly
non-exceptional.

### Producer-only services

For a service that only enqueues (never claims), the client surface
you'll touch:

```python
c.enqueue(kind=..., params=..., idempotency_key=...)   # → EnqueueResult
c.cancel(job_id)                                        # → Job
c.get_job(job_id, include_current_run=False)
c.list_jobs(status=..., kind=...)
c.list_job_runs(job_id)                                 # per-attempt history
```

### Schedules

```python
sch = c.create_schedule(kind="nightly-rollup", cron_expr="0 3 * * *")

# Manually trigger an extra run (doesn't move next_fire_at):
c.fire_schedule(sch.id)
```

## Worker quick-start

```python
import os, time
from yourapp.foreman import Client
from yourapp.foreman.worker import Worker, FailError, PermanentError

c = Client(os.environ["FOREMAN_ENDPOINT"])
w = Worker(c, worker_id=f"emailer-{os.getpid()}", lease_sec=60,
           on_error=lambda e: log.exception(e))

@w.handle("send-email")
def send_email(job, progress, cancel):
    args = job.params or {}
    progress.set(0, 1, "sending")
    if cancel.is_set():
        return None  # Worker will Fail; server terminalizes as cancelled
    if not args.get("to"):
        raise PermanentError("missing 'to' field")
    smtp.send(args["to"], args["body"])
    return {"sent": True}

w.run()  # blocks until SIGINT / SIGTERM / w.stop()
```

### What Worker does for you

- Claim across all registered kinds with empty-queue backoff
- Background heartbeat thread pinned to `lease_sec / 3` by default
- Progress reporting that piggybacks on the next heartbeat — no extra
  request per progress tick
- Cooperative cancel observation (see below)
- `complete()` on handler return, `fail()` on handler exception
- Plain `Exception` is caught — handler crashes don't tear down the
  worker, they become a `Fail` call
- Graceful shutdown — SIGINT / SIGTERM / `w.stop()` end the loop;
  in-flight handler terminalizes before `run()` returns

One Worker handles one job at a time. For parallel execution within a
process, spawn multiple Workers in their own threads — the server side
coordinates via `SELECT ... FOR UPDATE SKIP LOCKED`. Multiple processes
work the same way.

## Cooperative cancellation

`c.cancel(job_id)` on a *pending* job flips it to `cancelled`
immediately. On a *running* job, the server sets `cancel_requested =
True` and the worker's next heartbeat response surfaces it — Worker
sets the `cancel` :class:`threading.Event` handed to the handler.
Handlers should check it:

```python
@w.handle("long-import")
def long_import(job, progress, cancel):
    for i, batch in enumerate(batches):
        if cancel.is_set():
            return None  # Worker will treat this as cancellation
        process(batch)
        progress.set(i + 1, len(batches))
    return {"ok": True}
```

If `cancel.is_set()` is True at handler exit (whether you returned
or raised), Worker calls `Fail("cancelled by request", retryable=False)`.
The server, seeing `cancel_requested=True`, terminalizes the **job** as
`cancelled` regardless of attempts left. The run row stays `failed` —
only the parent job becomes `cancelled`.

## Retry semantics

How Worker turns a handler's exit into a terminal status:

| Handler exit | Resulting state |
|---|---|
| `return value` (no exception) | `complete` → job succeeded |
| `raise Exception(...)` (plain) | `Fail(retryable=True, backoff=default_backoff_sec)` → bounces to pending; terminalizes `failed` once attempts exhaust |
| `raise PermanentError("...")` | `Fail(retryable=False)` → terminalizes immediately |
| `raise FailError(message, retryable, backoff_sec)` | Explicit control over retryability + backoff |
| Cancellation observed | `Fail` → server terminalizes as `cancelled` |

## Error handling

Every non-2xx response (other than the Enqueue 409 collision) raises
`HTTPError`:

```python
from yourapp.foreman import HTTPError

try:
    job = c.get_job("job_nonexistent")
except HTTPError as exc:
    if exc.status_code == 404:
        ...
    elif exc.status_code == 409:
        ...
    else:
        log.error("foreman %s: %s %s", exc.op, exc.status_code, exc.message)
```

`HTTPError.message` is the server's `{"error": "..."}` field when
present, otherwise `"foreman: <op> responded N"`.

## No-op mode

`Client("")` returns a client whose every method is a no-op success
returning zero values. Useful when Foreman is configured per-environment
and you don't want each caller to branch on "is it enabled here":

```python
c = Client(os.environ.get("FOREMAN_ENDPOINT", ""))  # empty in dev = no-op
c.enqueue(kind="ping")  # silently returns EnqueueResult()
```

## Worker tuning

Defaults are picked for the common case; override what matters:

```python
w = Worker(
    c,
    worker_id="emailer-1",

    # Restrict — by default Worker claims any kind it has a handler for.
    kinds=["send-email"],
    queues=["transactional"],

    # Server-side lease length, in seconds. Default 60.
    lease_sec=60,
    # How often we heartbeat. Default lease_sec / 3.
    heartbeat_interval=20.0,
    # Empty-queue backoff. Default 5.0s.
    poll_interval=5.0,
    # Retry delay for plain-exception Fails. Default 30.
    default_backoff_sec=30,

    # Background-loop errors go here. None → swallow silently.
    on_error=lambda exc: log.exception("worker bg: %s", exc),
)
```

### Embedding in a larger app

If your process already owns signal handling, pass
`install_signal_handlers=False` to `run()` and drive the loop via
`w.stop()` from your own shutdown hook:

```python
w.run(install_signal_handlers=False)
# elsewhere:
atexit.register(w.stop)
```

## Smoke test

Drop the files into your project, then verify the wire is right:

```python
c = Client(endpoint)

res = c.enqueue(kind="ping")
claimed = c.claim(kinds=["ping"], worker_id="smoke")
c.heartbeat(claimed[1].id, worker_id="smoke")
c.complete(claimed[1].id, worker_id="smoke", result={"ok": True})

runs = c.list_job_runs(res.job.id)
assert runs[0].status == "succeeded"
```

If all five calls succeed, the client is wired correctly.
