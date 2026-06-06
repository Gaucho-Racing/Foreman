package model

import "time"

const (
	// RunStatusRunning — the worker is currently leasing this attempt.
	RunStatusRunning = "running"
	// RunStatusSucceeded — the worker called Complete; Result is set.
	RunStatusSucceeded = "succeeded"
	// RunStatusFailed — the worker called Fail; Error is set. The parent
	// Job may still bounce to pending (retry) or terminalize to failed.
	RunStatusFailed = "failed"
	// RunStatusAbandoned — the reaper swept this attempt because its
	// lease expired without a Complete/Fail. Same parent-job branching
	// as Failed, just driven by the reaper rather than a worker.
	RunStatusAbandoned = "abandoned"
)

// JobRun is one attempt at running a Job — the source of truth for all
// transient and per-attempt state: which worker holds the lease, how it
// progressed, what error or result it ended with. The parent Job points
// at the *definition*; runs answer "what happened on attempt N".
//
// Invariant: at most one row per (job_id, status='running'). Enforced by
// service logic, not the DB — Postgres can't express partial unique
// indexes via GORM tags. Composite UNIQUE on (job_id, attempt) catches
// the broader "one row per attempt".
type JobRun struct {
	ID    string `json:"id" gorm:"primaryKey"`
	JobID string `json:"job_id" gorm:"uniqueIndex:idx_job_runs_job_attempt;not null"`
	// Attempt is 1-based and increases monotonically per job — set to
	// Job.AttemptCount at the moment of claim.
	Attempt int `json:"attempt" gorm:"uniqueIndex:idx_job_runs_job_attempt;not null"`

	WorkerID string `json:"worker_id" gorm:"not null"`
	Status   string `json:"status" gorm:"not null"`

	// LeaseExpiresAt is meaningful only while status='running'. Reaper
	// uses this to detect abandoned leases. Set to NULL on any terminal
	// transition. The supporting index is partial on status='running'
	// and is created in database.Init's applySchemaExtensions — GORM
	// tags can't express partial indexes.
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`

	ProgressCurrent int64  `json:"progress_current" gorm:"not null;default:0"`
	ProgressTotal   int64  `json:"progress_total" gorm:"not null;default:0"`
	ProgressMessage string `json:"progress_message,omitempty"`

	Result JSON   `json:"result,omitempty" gorm:"type:jsonb"`
	Error  string `json:"error,omitempty"`

	StartedAt  time.Time  `json:"started_at" gorm:"not null"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`

	CreatedAt time.Time `json:"created_at" gorm:"autoCreateTime;precision:6"`
	UpdatedAt time.Time `json:"updated_at" gorm:"autoUpdateTime;precision:6"`
}

func (JobRun) TableName() string { return "job_runs" }

func (r JobRun) IsTerminal() bool {
	return r.Status == RunStatusSucceeded || r.Status == RunStatusFailed || r.Status == RunStatusAbandoned
}
