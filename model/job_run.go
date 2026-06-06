package model

import "time"

const (
	// RunStatusRunning — the worker is currently leasing this attempt.
	RunStatusRunning = "running"
	// RunStatusSucceeded — the worker called Complete.
	RunStatusSucceeded = "succeeded"
	// RunStatusFailed — the worker called Fail. The parent Job may still
	// be pending (retry) or failed (terminal) depending on retryability.
	RunStatusFailed = "failed"
	// RunStatusAbandoned — the reaper swept this attempt because its
	// lease expired without a Complete/Fail. The parent Job is bounced
	// back to pending (retry) or marked failed (exhausted) — same
	// branching as Fail, just driven by the reaper.
	RunStatusAbandoned = "abandoned"
)

// JobRun records one attempt at running a Job. A Job grows one JobRun
// per claim; the row captures everything the dashboard needs to answer
// "what happened on this attempt": which worker, when it started and
// finished, last-known progress, terminal error/result.
//
// The parent Job continues to carry the latest worker_id / lease /
// progress as denormalized state so claim + heartbeat don't have to
// JOIN — JobRun is the audit trail, not the source of truth for "what
// is this job doing right now".
type JobRun struct {
	ID    string `json:"id" gorm:"primaryKey"`
	JobID string `json:"job_id" gorm:"index;uniqueIndex:idx_job_runs_job_attempt;not null"`
	// Attempt mirrors Job.Attempt at claim time. Combined with JobID it's
	// unique — a single claim is one attempt.
	Attempt  int    `json:"attempt" gorm:"uniqueIndex:idx_job_runs_job_attempt;not null"`
	WorkerID string `json:"worker_id" gorm:"not null"`
	Status   string `json:"status" gorm:"index;not null"`

	ProgressCurrent int64  `json:"progress_current" gorm:"not null;default:0"`
	ProgressTotal   int64  `json:"progress_total" gorm:"not null;default:0"`
	ProgressMessage string `json:"progress_message,omitempty"`

	Error  string `json:"error,omitempty"`
	Result JSON   `json:"result,omitempty" gorm:"type:jsonb"`

	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty" gorm:"index"`

	StartedAt  time.Time  `json:"started_at" gorm:"not null"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`

	CreatedAt time.Time `json:"created_at" gorm:"autoCreateTime;precision:6"`
	UpdatedAt time.Time `json:"updated_at" gorm:"autoUpdateTime;precision:6"`
}

func (JobRun) TableName() string { return "job_runs" }

func (r JobRun) IsTerminal() bool {
	return r.Status == RunStatusSucceeded || r.Status == RunStatusFailed || r.Status == RunStatusAbandoned
}
