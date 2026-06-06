# Foreman

[![Build](https://img.shields.io/github/actions/workflow/status/Gaucho-Racing/Foreman/build.yml?branch=main)](https://github.com/Gaucho-Racing/Foreman/actions/workflows/build.yml)
[![Release](https://img.shields.io/github/v/release/Gaucho-Racing/Foreman?label=release)](https://github.com/Gaucho-Racing/Foreman/releases/latest)
[![Image](https://img.shields.io/badge/ghcr.io-foreman-blue?logo=docker)](https://github.com/Gaucho-Racing/Foreman/pkgs/container/foreman)
[![License](https://img.shields.io/github/license/Gaucho-Racing/Foreman)](./LICENSE)

Lightweight job orchestration backed by Postgres. Producers enqueue jobs
with an optional idempotency key; workers claim them by kind, lease them,
report progress + outcome. A reaper sweeps abandoned leases back to
pending so a worker crash never leaves work stuck.

Originally extracted from [Gaucho Racing's Mapache](https://github.com/Gaucho-Racing/Mapache);
now standalone so other GR projects (and anyone else) can drop it in.

## Why

Most teams don't need Sidekiq / Resque / Temporal — they need "kick off
a job from one service, have another service pick it up, get retries +
idempotency + cancel for free." Foreman is one binary + one Postgres
table and goes from zero to running in a few seconds.

Concretely:

- **Idempotent enqueue.** A unique `(kind, idempotency_key)` index turns
  "we already kicked off X" tracking tables into one extra column.
- **Atomic claim.** `SELECT ... FOR UPDATE SKIP LOCKED` so N workers
  never grab the same job.
- **Lease + heartbeat.** Workers must heartbeat or the reaper hands the
  job to someone else.
- **Retries with backoff.** Failed-but-retryable jobs return to pending
  with a per-call backoff delay.
- **Cooperative cancel.** Pending jobs flip to cancelled immediately;
  running jobs see `cancel_requested` on their next heartbeat.
- **SSE stream.** `GET /foreman/events/:id` pushes the job's current
  state until terminal, for dashboards that want live progress.

## Run it

```sh
# Containerized (foreman + Postgres):
make docker-up
curl localhost:7011/foreman/ping

# Or against your own Postgres:
cp .env.example .env  # edit if needed
make run
```

## Quick tour of the API

```sh
# Enqueue
curl -X POST localhost:7011/foreman/jobs \
  -H 'Content-Type: application/json' \
  -d '{"kind":"send-email","params":{"to":"x@y.com"},"max_attempts":3,"idempotency_key":"email-42"}'

# Claim one
curl -X POST localhost:7011/foreman/claim \
  -H 'Content-Type: application/json' \
  -d '{"kinds":["send-email"],"worker_id":"worker-1","lease_seconds":60}'

# Heartbeat / progress
curl -X POST localhost:7011/foreman/jobs/<id>/heartbeat \
  -d '{"worker_id":"worker-1","progress_current":12,"progress_total":100,"lease_seconds":60}'

# Complete
curl -X POST localhost:7011/foreman/jobs/<id>/complete \
  -d '{"worker_id":"worker-1","result":{"sent":true}}'

# Fail (retryable)
curl -X POST localhost:7011/foreman/jobs/<id>/fail \
  -d '{"worker_id":"worker-1","error":"smtp timeout","retryable":true,"backoff_seconds":30}'

# Cancel
curl -X POST localhost:7011/foreman/jobs/<id>/cancel

# Per-attempt history (one row per claim)
curl localhost:7011/foreman/jobs/<id>/runs

# Stream state (SSE; one event per change; closes on terminal)
curl -N localhost:7011/foreman/events/<id>
```

## Job runs

Every time a worker claims a job, Foreman writes a `job_runs` row
recording that attempt — worker id, start/finish, last-known progress,
terminal error or result. The parent job row keeps the "what is this
doing right now" denorm (latest worker, current lease), but `job_runs`
is the immutable audit trail. A job that succeeded on its third try
will have three rows: two `failed`, one `succeeded`. A job whose worker
crashed mid-attempt gets a row marked `abandoned` by the reaper.

```sh
$ curl localhost:7011/foreman/jobs/job_.../runs
[
  { "attempt": 1, "worker_id": "w-1", "status": "failed",    "error": "smtp timeout", ... },
  { "attempt": 2, "worker_id": "w-2", "status": "succeeded", "result": {"sent": true}, ... }
]
```

Full route list: see `api/api.go`.

## Configuration

| Env                              | Default     | Notes |
|----------------------------------|-------------|-------|
| `ENV`                            | `PROD`      | Set to `DEV` for pretty logs + dev-mode Gin. |
| `PORT`                           | `7011`      | HTTP listen port. |
| `DATABASE_HOST`                  | `localhost` | Postgres host. |
| `DATABASE_PORT`                  | `5432`      | Postgres port. |
| `DATABASE_NAME`                  | `foreman`   | Database name. |
| `DATABASE_USER`                  | `postgres`  | User. |
| `DATABASE_PASSWORD`              | `password`  | Password. |
| `FOREMAN_REAPER_INTERVAL_SEC`    | `10`        | Reaper sweep cadence. |
| `FOREMAN_DEFAULT_LEASE_SEC`      | `60`        | Lease length when claim omits one. |
| `FOREMAN_SCHEDULER_INTERVAL_SEC` | `1`         | Scheduler tick cadence. |
| `FOREMAN_RETENTION_DAYS`         | `0`         | Delete terminal jobs older than this (0 = keep forever). |

## Layout

```
api/         HTTP routes (Gin)
config/      env loading + service info
database/    GORM bootstrap + auto-migration
model/       Job (and soon JobRun) row types
service/     enqueue/claim/heartbeat/complete/fail + reaper
pkg/logger/  zap setup
```

## Releasing

```sh
make release V=0.2.0     # or: ./scripts/release.sh 0.2.0
```

Preflights main + clean tree, bumps `Version` in `config/config.go`,
commits, tags `v0.2.0`, pushes, and cuts the GitHub release. The build
workflow picks up the tag and publishes `ghcr.io/gaucho-racing/foreman`
at `:0.2.0`, `:0.2`, and `:0` — pin at whichever specificity you want.

## License

MIT — see [LICENSE](./LICENSE).
