# Foreman Go client

Drop-in HTTP client + optional Worker loop for Foreman. Two files,
standard library only.

| File | What it gives you |
|---|---|
| [`foreman.go`](./foreman.go) | `Client` — raw HTTP wrapper for every API endpoint |
| [`worker.go`](./worker.go)   | `Worker` — claim → heartbeat → handle → terminate loop |

Drop just `foreman.go` if you only need to *kick off* jobs from a
producer service. Drop both if you also need to *run* them.

## Install

Copy the files into your project, e.g. `internal/foreman/`:

```
internal/foreman/
├── foreman.go
└── worker.go   # optional
```

Both files declare `package foreman`. Import normally:

```go
import "yourapp/internal/foreman"
```

No `go get` needed — net/http is the only dependency.

## Client quick-start

```go
package main

import (
    "context"
    "encoding/json"
    "log"

    "yourapp/internal/foreman"
)

func main() {
    c := foreman.New("http://foreman.local:7011")
    ctx := context.Background()

    // Enqueue
    res, err := c.Enqueue(ctx, foreman.EnqueueRequest{
        Kind:           "send-email",
        Params:         json.RawMessage(`{"to":"x@y.com"}`),
        IdempotencyKey: foreman.Ptr("email-42"),  // re-sends are no-ops
        MaxAttempts:    3,
    })
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("created=%v id=%s", res.Created, res.Job.ID)
}
```

The 409 idempotency-key collision is **not** an error — `Enqueue`
returns `EnqueueResult{Created: false, Job: existing}` so re-sends
are explicitly non-exceptional.

### Producer-only services

For a service that only enqueues (never claims), the client surface
you'll touch:

```go
c.Enqueue(ctx, EnqueueRequest{...})    // → EnqueueResult
c.Cancel(ctx, jobID)                    // → Job
c.GetJob(ctx, jobID, false)             // false → omit current_run
c.ListJobs(ctx, JobsFilter{...})
c.ListJobRuns(ctx, jobID)               // per-attempt history
```

### Schedules

```go
sch, _ := c.CreateSchedule(ctx, foreman.ScheduleRequest{
    Kind:     "nightly-rollup",
    CronExpr: "0 3 * * *",   // 03:00 daily
})

// Manually trigger an extra run (doesn't move next_fire_at):
c.FireSchedule(ctx, sch.ID)
```

## Worker quick-start

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "yourapp/internal/foreman"
)

func main() {
    c := foreman.New(os.Getenv("FOREMAN_ENDPOINT"))

    w := &foreman.Worker{
        Client:   c,
        WorkerID: hostnamePlusPid(),
        LeaseSec: 60,
        OnError:  func(err error) { log.Printf("worker: %v", err) },
    }

    w.Handle("send-email", func(ctx context.Context, job foreman.Job, p *foreman.Progress) (json.RawMessage, error) {
        var args struct{ To, Body string }
        if err := json.Unmarshal(job.Params, &args); err != nil {
            // Bad params won't fix themselves — non-retryable.
            return nil, &foreman.FailError{Message: err.Error(), Retryable: false}
        }
        p.Set(0, 1, "sending")
        if err := smtp.Send(args.To, args.Body); err != nil {
            return nil, err  // plain error → retryable by default
        }
        return json.RawMessage(`{"sent":true}`), nil
    })

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    if err := w.Run(ctx); err != nil {
        log.Fatal(err)
    }
}
```

### What Worker does for you

- Claim across all registered kinds with empty-queue backoff
- Background heartbeat ticker pinned to `LeaseSec/3` by default
- Progress reporting that piggybacks on the next heartbeat — no extra
  request per progress tick
- Cooperative cancel observation (see below)
- `Complete` on handler return, `Fail` on handler error
- Panic recovery — a panicking handler becomes a non-retryable `Fail`
- Graceful shutdown — cancel the parent ctx and `Run` returns after
  the in-flight handler terminalizes

One Worker handles one job at a time. For parallel execution within
a process, spawn N Workers in goroutines — the server side coordinates
via `SELECT ... FOR UPDATE SKIP LOCKED`.

## Cooperative cancellation

`c.Cancel(jobID)` on a *pending* job flips it to `cancelled`
immediately. On a *running* job, the server sets `cancel_requested =
true` and the worker's next `Heartbeat` response surfaces it — Worker
cancels the handler's context. Handlers should respect `ctx.Done()`:

```go
w.Handle("long-import", func(ctx context.Context, job foreman.Job, p *foreman.Progress) (json.RawMessage, error) {
    for i, batch := range batches {
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        default:
        }
        process(batch)
        p.Set(int64(i+1), int64(len(batches)), "")
    }
    return json.RawMessage(`{"ok":true}`), nil
})
```

When the worker `Fail`s a run on a cancel-requested job, the server
terminalizes the **job** as `cancelled` (not `failed`), regardless of
attempts left. The run row itself stays `failed` — it failed from the
worker's perspective. Only the parent job becomes `cancelled`.

## Retry semantics

How Worker turns a handler's return value into a terminal status:

| Handler exit | Resulting state |
|---|---|
| `return result, nil` | `Complete` → job succeeded |
| `return nil, err` (plain) | `Fail(retryable=true, backoff=DefaultBackoffSec)` → bounces to pending; terminalizes `failed` once attempts exhaust |
| `return nil, fmt.Errorf("...: %w", foreman.ErrPermanent)` | `Fail(retryable=false)` → terminalizes immediately |
| `return nil, &foreman.FailError{Message, Retryable, BackoffSec}` | Explicit control over retryability + backoff |
| Panic | `Fail(retryable=false)` with the panic value in the error field |
| Cancellation observed | `Fail` → server terminalizes as `cancelled` |

## Error handling

Every non-2xx response (other than the Enqueue 409 collision)
returns `*foreman.HTTPError`:

```go
job, err := c.GetJob(ctx, "job_nonexistent", false)
if err != nil {
    if foreman.IsNotFound(err) {
        // 404 — gone or never existed
    } else if foreman.IsConflict(err) {
        // 409
    } else {
        // network, 5xx, etc.
        var he *foreman.HTTPError
        if errors.As(err, &he) {
            log.Printf("status=%d msg=%s", he.StatusCode, he.Message)
        }
    }
}
```

`HTTPError.Message` is the server's `{"error": "..."}` field when
present, otherwise a generic `"foreman: <op> responded N"`.

## No-op mode

`foreman.New("")` returns a client whose every method is a no-op
success returning zero values. Useful when Foreman is configured
per-environment and you don't want each caller to branch on "is it
enabled here":

```go
c := foreman.New(os.Getenv("FOREMAN_ENDPOINT"))  // empty in dev = no-op
c.Enqueue(ctx, foreman.EnqueueRequest{...})       // silently succeeds
```

## Worker tuning

Defaults are picked for the common case; override what matters:

```go
w := &foreman.Worker{
    Client:            c,
    WorkerID:          "emailer-1",

    // Restrict — by default Worker claims any kind it has a handler for.
    Kinds:  []string{"send-email"},
    Queues: []string{"transactional"},

    // Server-side lease length. Default 60s.
    LeaseSec: 60,
    // How often we heartbeat. Default LeaseSec/3.
    HeartbeatInterval: 20 * time.Second,
    // Empty-queue backoff. Default 5s.
    PollInterval: 5 * time.Second,
    // Retry delay for plain-error Fails. Default 30s.
    DefaultBackoffSec: 30,

    // Background-loop errors go here. nil → swallow silently.
    OnError: func(err error) { metrics.WorkerErrors.Inc() },
}
```

## Smoke test

Drop the files into your project, then verify the wire is right:

```go
c := foreman.New(endpoint)
ctx := context.Background()

res, _ := c.Enqueue(ctx, foreman.EnqueueRequest{Kind: "ping"})
cl, _ := c.Claim(ctx, foreman.ClaimRequest{
    Kinds: []string{"ping"}, WorkerID: "smoke",
})
_, _ = c.Heartbeat(ctx, cl.Run.ID, foreman.HeartbeatRequest{WorkerID: "smoke"})
_, _ = c.Complete(ctx, cl.Run.ID, foreman.CompleteRequest{
    WorkerID: "smoke", Result: json.RawMessage(`{"ok":true}`),
})
runs, _ := c.ListJobRuns(ctx, res.Job.ID)
// runs[0].Status should be "succeeded"
```

If all five calls succeed, the client is wired correctly.
