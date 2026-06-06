# Foreman clients

Single-file, drop-in clients for the Foreman REST API. Each file in a
`clients/<language>/` directory is meant to be copied into your project
as-is — no vendoring of the rest of the Foreman repo required.

Two files per language, each meant to be copied into your project:

| Language | Files | Runtime deps |
|---|---|---|
| Go     | [`go/foreman.go`](./go/foreman.go), [`go/worker.go`](./go/worker.go) | standard library only |
| Python | [`python/foreman.py`](./python/foreman.py), [`python/worker.py`](./python/worker.py) | `requests` |

`foreman.{go,py}` is the raw HTTP client — enqueue, claim, heartbeat,
complete, fail, cancel, schedules. Drop it in if all you need is to
*kick off jobs* from a producer service.

`worker.{go,py}` is the canonical claim → heartbeat → handle → terminate
loop on top of the client — drop it in if you also need to *run jobs*.
You write the handler bodies; everything else (heartbeat ticker,
progress reporting, cooperative cancel observation, retry classification,
panic recovery, graceful shutdown) is handled for you.

## Conventions

These are intentionally not full SDKs maintained as their own packages.
The point is:

- **One file per concern per language** so you can read it end-to-end
  in 10 minutes and audit before dropping into your codebase.
- **No third-party dependencies** beyond the standard HTTP library
  (`net/http` in Go, `requests` in Python).
- **Same shape across languages.** Each client method maps 1:1 onto a
  REST endpoint; type names match the API's JSON keys.

If you want a properly versioned module — semver tags, go.mod
versioning, separate testing — wrap one of these in your own repo.
We're keeping these here as starting points, not as a maintained SDK.

## API shape

Every method maps onto exactly one REST endpoint. See the
[main README](../README.md) and [`api/api.go`](../api/api.go) for the
full surface; in short:

| Method | Endpoint |
|---|---|
| Enqueue              | `POST /foreman/jobs`                 |
| Claim                | `POST /foreman/jobs/claim`           |
| Heartbeat            | `POST /foreman/runs/:id/heartbeat`   |
| Complete             | `POST /foreman/runs/:id/complete`    |
| Fail                 | `POST /foreman/runs/:id/fail`        |
| Cancel               | `POST /foreman/jobs/:id/cancel`      |
| GetJob, ListJobs     | `GET  /foreman/jobs[?...&include=current_run]` |
| GetRun, ListRuns     | `GET  /foreman/runs[?filters]`       |
| ListJobRuns          | `GET  /foreman/jobs/:id/runs`        |
| CreateSchedule, …    | `*    /foreman/schedules[/:id][/fire]` |

## Error handling

Both clients fold the server's `{"error": "..."}` envelope into the
language-native error type:

- **Go**: returns `*foreman.HTTPError{Op, StatusCode, Message}`. Helpers
  `foreman.IsConflict(err)` / `foreman.IsNotFound(err)` for the two
  cases worth special-casing.
- **Python**: raises `foreman.HTTPError(op, status_code, message)`.

The 409 collision on `Enqueue` is the one non-error 4xx: both clients
return an `EnqueueResult` with `created=False` and the existing `Job`
attached, so idempotent re-sends are not exceptional.

## No-op mode

Passing an empty endpoint (`Client("")`) makes every client method a
no-op success returning zero values. Useful for deployments where
Foreman is configured per-environment and you don't want every caller
to branch on "is Foreman enabled here."

## Running jobs: the Worker

Both `worker.go` and `worker.py` wrap the canonical loop:

```
forever:
  claim a job for any registered kind
  spawn heartbeat ticker (auto-progress + cancel observation)
  call handler(ctx_or_event, job, progress)
  on success → Complete; on error → Fail (retryable by default)
```

### Go

```go
w := &foreman.Worker{Client: c, WorkerID: "emailer-1", LeaseSec: 60}
w.Handle("send-email", func(ctx context.Context, job foreman.Job, p *foreman.Progress) (json.RawMessage, error) {
    p.Set(0, 1, "sending")
    if err := smtp.Send(...); err != nil {
        return nil, err                                    // retryable
    }
    return json.RawMessage(`{"sent":true}`), nil
})
w.Run(ctx)
```

### Python

```python
w = foreman.Worker(c, worker_id="emailer-1", lease_sec=60)

@w.handle("send-email")
def send_email(job, progress, cancel):
    progress.set(0, 1, "sending")
    if cancel.is_set():
        return None                                        # cancelled
    smtp.send(...)
    return {"sent": True}

w.run()  # blocks until SIGINT / SIGTERM
```

### Cooperative cancellation

Calling `Cancel(job_id)` on a *pending* job flips it to `cancelled`
immediately. On a *running* job, the server sets `cancel_requested=true`
and the worker's next `Heartbeat` response surfaces it — the Worker
abstraction cancels the handler context (Go) or sets the cancel event
(Python). Handlers should check this signal and bail promptly.

When the worker `Fail`s a run on a job that has `cancel_requested=true`,
the server terminalizes the job as `cancelled` (not `failed`), regardless
of attempts left. The run row itself stays `failed` — it failed from
the worker's perspective. Only the parent job becomes `cancelled`.

### Retry semantics

| Handler exit | Resulting job state |
|---|---|
| Returns value (no error / no exception) | `Complete` → succeeded |
| Returns error (Go) or raises (Python) — plain | `Fail(retryable=true, backoff=DefaultBackoffSec)` → bounces to pending, terminalizes as `failed` once attempts exhaust |
| Wraps with `foreman.ErrPermanent` (Go) / raises `PermanentError` (Python) | `Fail(retryable=false)` → terminalizes immediately |
| Returns `*FailError` (Go) / raises `FailError` (Python) | `Fail` with explicit retryable + backoff |
| Panic (Go) / unhandled crash inside `safeCall` | `Fail(retryable=false)` with panic value in error field |
| Cancellation observed | `Fail` → server terminalizes as `cancelled` |

### Concurrency

One Worker handles one job at a time. For parallel execution within a
process, spawn N Workers in goroutines / threads — the server side
coordinates via `SELECT ... FOR UPDATE SKIP LOCKED` so they never grab
the same job.

## Smoke testing your copy

After dropping the files into your project, the canonical sanity checks:

```
client only:    enqueue → claim → heartbeat → complete → list_runs
client+worker:  enqueue → Worker.Run → confirm 'succeeded'
                enqueue + cancel mid-flight → confirm 'cancelled'
```

Both clients here have been smoked end-to-end against a live Foreman
covering happy path, retryable failure, permanent failure, and
cancellation through the heartbeat-surfaced `cancel_requested` signal.
