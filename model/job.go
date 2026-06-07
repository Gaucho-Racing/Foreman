package model

import "time"

const (
	// StatusPending — claimable. Either never claimed, or bounced back
	// after a retryable failure / abandoned lease.
	StatusPending = "pending"
	// StatusActive — a worker holds a JobRun for this job. Heartbeats and
	// progress live on the run, not here.
	StatusActive = "active"
	// StatusSucceeded — terminal. A JobRun completed and its result is
	// denormed onto Job.Result for fast list rendering.
	StatusSucceeded = "succeeded"
	// StatusFailed — terminal. Either retries were exhausted or a worker
	// reported a non-retryable failure.
	StatusFailed = "failed"
	// StatusCancelled — terminal. Pending jobs cancel immediately; active
	// jobs flip via Fail once the worker observes CancelRequested.
	StatusCancelled = "cancelled"
)

// Job is the *definition* of a unit of work + its overall lifecycle
// outcome. It deliberately carries no transient run state (no worker_id,
// lease, progress, or per-attempt error) — that lives on JobRun, which
// is queryable via job_runs WHERE job_id = ? AND status = 'running'.
//
// The only denorms here are by design:
//   - Status: claim's hot-path filter, can't afford a join on every claim.
//   - AttemptCount: count(job_runs) for this job, cached so the claim
//     and reaper can branch retry vs. terminal without a subquery.
//   - Result: copied from the winning run so the dashboard list view
//     can show success output without joining.
type Job struct {
	ID      string `json:"id" gorm:"primaryKey"`
	Kind    string `json:"kind" gorm:"uniqueIndex:idx_jobs_kind_idem;not null"`
	Queue   string `json:"queue" gorm:"index;not null;default:default"`
	Service string `json:"service" gorm:"index"`

	// IdempotencyKey is unique within a kind. Nullable: ad-hoc jobs may
	// skip dedup. Postgres' default NULLS DISTINCT lets multiple
	// non-deduped enqueues coexist.
	IdempotencyKey *string `json:"idempotency_key,omitempty" gorm:"uniqueIndex:idx_jobs_kind_idem"`

	// Params is the input payload — frozen at enqueue time.
	Params      JSON `json:"params,omitempty" gorm:"type:jsonb"`
	Priority    int  `json:"priority" gorm:"not null;default:0"`
	MaxAttempts int  `json:"max_attempts" gorm:"not null;default:1"`

	// ScheduledAt gates claim visibility — a job is claimable only once
	// now >= scheduled_at. Used for delayed enqueue and retry backoff.
	ScheduledAt *time.Time `json:"scheduled_at,omitempty" gorm:"index"`

	Status          string `json:"status" gorm:"index;not null"`
	CancelRequested bool   `json:"cancel_requested" gorm:"not null;default:false"`

	// AttemptCount is a denormalized count(job_runs WHERE job_id=?). Kept
	// here so retry decisions (in Fail and the reaper) don't have to
	// count rows on every transition.
	AttemptCount int `json:"attempt_count" gorm:"not null;default:0"`

	// Result is the winning run's result, copied here on Complete so the
	// jobs list can render outcomes without joining job_runs.
	Result JSON `json:"result,omitempty" gorm:"type:jsonb"`

	EnqueuedAt  time.Time  `json:"enqueued_at" gorm:"autoCreateTime;precision:6"`
	StartedAt   *time.Time `json:"started_at,omitempty"`   // first claim across all attempts
	CompletedAt *time.Time `json:"completed_at,omitempty"` // terminal time
	UpdatedAt   time.Time  `json:"updated_at" gorm:"autoUpdateTime;precision:6"`
}

func (Job) TableName() string { return TableJobs() }

func (j Job) IsTerminal() bool {
	return j.Status == StatusSucceeded || j.Status == StatusFailed || j.Status == StatusCancelled
}
