# Foreman clients

Single-file, drop-in clients for the Foreman REST API. Each language
directory contains the client plus an optional Worker loop, meant to
be copied into your project as-is — no vendoring of the rest of the
Foreman repo required.

## Languages

| Language | Files | Runtime deps | Docs |
|---|---|---|---|
| Go     | [`go/foreman.go`](./go/foreman.go), [`go/worker.go`](./go/worker.go) | standard library only | [`go/README.md`](./go/README.md) |
| Python | [`python/foreman.py`](./python/foreman.py), [`python/worker.py`](./python/worker.py) | `requests` | [`python/README.md`](./python/README.md) |

Each language gets its own README with install, quick-starts, retry
semantics, cancellation flow, and tuning knobs.

## What you drop in

Two files per language, layered:

- **`foreman.{go,py}`** — raw HTTP client. Drop this if you only need
  to *kick off* jobs from a producer service. Methods map 1:1 onto
  REST endpoints; type names match the JSON keys.
- **`worker.{go,py}`** — the canonical claim → heartbeat → handle →
  terminate loop, on top of the client. Drop this if you also need
  to *run* jobs. You write the handler bodies; everything else
  (heartbeat ticker, progress reporting, cooperative cancel
  observation, retry classification, panic recovery, graceful
  shutdown) is handled for you.

## Conventions

These are intentionally not full SDKs maintained as their own packages.
The point is:

- **One file per concern per language** so you can read it end-to-end
  in 10 minutes and audit before dropping into your codebase.
- **No third-party dependencies** beyond the standard HTTP library
  (`net/http` in Go, `requests` in Python).
- **Same shape across languages.** Each client method maps 1:1 onto
  a REST endpoint; type names match the API's JSON keys; semantic
  conventions (Enqueue's 409-as-success, 204-on-empty-claim,
  cancel-wins-over-retry) are identical.

If you want a properly versioned module — semver tags, separate
testing — wrap one of these in your own repo. We're keeping these
here as starting points, not as a maintained SDK.

## API surface

Every client method maps onto exactly one REST endpoint. See
[`api/api.go`](../api/api.go) for the route definitions; in short:

| Method | Endpoint |
|---|---|
| `Enqueue`               | `POST /foreman/jobs`                 |
| `Claim`                 | `POST /foreman/jobs/claim`           |
| `Heartbeat`             | `POST /foreman/runs/:id/heartbeat`   |
| `Complete`              | `POST /foreman/runs/:id/complete`    |
| `Fail`                  | `POST /foreman/runs/:id/fail`        |
| `Cancel`                | `POST /foreman/jobs/:id/cancel`      |
| `GetJob`, `ListJobs`    | `GET  /foreman/jobs[?...&include=current_run]` |
| `GetRun`, `ListRuns`    | `GET  /foreman/runs[?filters]`       |
| `ListJobRuns`           | `GET  /foreman/jobs/:id/runs`        |
| `CreateSchedule`, …     | `*    /foreman/schedules[/:id][/fire]` |

## Adding a new language

Match the existing files' shape:

1. `clients/<lang>/foreman.<ext>` — `Client` with one method per
   endpoint. 409 on Enqueue → non-error `EnqueueResult`. 204 on Claim
   → null/nil. Everything else → typed `HTTPError`. Empty endpoint
   → no-op success.
2. `clients/<lang>/worker.<ext>` — Worker with handler registry,
   background heartbeat loop reading `cancel_requested` off the
   heartbeat response, `Progress` with piggyback semantics, panic
   recovery, retry classification (`ErrPermanent` / `FailError`),
   graceful shutdown.
3. `clients/<lang>/README.md` — install, client quick-start, worker
   quick-start, retry semantics, cancellation, error handling,
   no-op mode, smoke test.

Smoke against a live Foreman covering: happy path, retryable failure
with retry-then-succeed, permanent failure, and cancellation
mid-handler.
