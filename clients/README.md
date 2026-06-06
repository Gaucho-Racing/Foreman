# Foreman clients

Single-file, drop-in clients for the Foreman REST API. Each file in a
`clients/<language>/` directory is meant to be copied into your project
as-is — no vendoring of the rest of the Foreman repo required.

| Language | File | Runtime deps |
|---|---|---|
| Go     | [`go/foreman.go`](./go/foreman.go)     | standard library only |
| Python | [`python/foreman.py`](./python/foreman.py) | `requests` |

## Conventions

These clients are intentionally *not* full SDKs maintained as their own
packages. The point is:

- **One file per language** so you can read it end-to-end in 10 minutes
  and audit before dropping into your codebase.
- **No third-party dependencies** beyond the standard HTTP library
  (`net/http` in Go, `requests` in Python).
- **Same shape across languages.** Each method maps 1:1 onto a REST
  endpoint; type names match the API's JSON keys.

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

Passing an empty endpoint (`Client("")`) makes every method a no-op
success returning zero values. Useful for deployments where Foreman is
configured per-environment and you don't want every caller to branch on
"is Foreman enabled here."

## Smoke testing your copy

After dropping the file into your project, the canonical sanity check:

```
enqueue → claim → heartbeat → complete → list_runs
```

If all five succeed without errors, the wire format is right. Both
clients in this directory have been smoked end-to-end against a live
Foreman; the Go file also compiles under the main repo's `go vet` and
`go build`.
