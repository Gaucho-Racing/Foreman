package service

import (
	"errors"
	"time"

	"github.com/gaucho-racing/foreman/config"
	"github.com/gaucho-racing/foreman/database"
	"github.com/gaucho-racing/foreman/model"

	ulid "github.com/gaucho-racing/ulid-go"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	// ErrConflict is returned by Enqueue when a job with the same
	// (kind, idempotency_key) already exists; the existing job is returned
	// alongside so the caller can decide to skip.
	ErrConflict = errors.New("job already exists for idempotency key")
	// ErrNotFound is returned when no job matches the given id.
	ErrNotFound = errors.New("job not found")
	// ErrNotOwned is returned by lease-scoped mutations (heartbeat,
	// complete, fail) when the calling worker does not hold a running
	// JobRun for this job — it was reclaimed by the reaper, cancelled,
	// or already terminalized.
	ErrNotOwned = errors.New("job not running under this worker")
)

// ---------- Enqueue ----------

type EnqueueParams struct {
	Kind           string
	Queue          string
	Service        string
	IdempotencyKey *string
	Params         model.JSON
	Priority       int
	MaxAttempts    int
	ScheduledAt    *time.Time
}

func Enqueue(p EnqueueParams) (model.Job, error) {
	return enqueueTx(database.DB, p)
}

func enqueueTx(tx *gorm.DB, p EnqueueParams) (model.Job, error) {
	if p.Queue == "" {
		p.Queue = "default"
	}
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	job := model.Job{
		ID:             ulid.Make().Prefixed("job"),
		Kind:           p.Kind,
		Queue:          p.Queue,
		Service:        p.Service,
		IdempotencyKey: p.IdempotencyKey,
		Params:         p.Params,
		Priority:       p.Priority,
		MaxAttempts:    p.MaxAttempts,
		ScheduledAt:    p.ScheduledAt,
		Status:         model.StatusPending,
	}

	if p.IdempotencyKey == nil {
		if err := tx.Create(&job).Error; err != nil {
			return model.Job{}, err
		}
		return job, nil
	}

	res := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "kind"}, {Name: "idempotency_key"}},
		DoNothing: true,
	}).Create(&job)
	if res.Error != nil {
		return model.Job{}, res.Error
	}
	if res.RowsAffected == 0 {
		existing, err := getByIdempotency(tx, p.Kind, *p.IdempotencyKey)
		if err != nil {
			return model.Job{}, err
		}
		return existing, ErrConflict
	}
	return job, nil
}

func getByIdempotency(tx *gorm.DB, kind, key string) (model.Job, error) {
	var job model.Job
	if err := tx.Where("kind = ? AND idempotency_key = ?", kind, key).First(&job).Error; err != nil {
		return model.Job{}, err
	}
	return job, nil
}

// ---------- Claim ----------

type ClaimParams struct {
	Kinds    []string
	Queues   []string
	WorkerID string
	LeaseSec int
}

// ClaimResult is what a worker gets back when a claim succeeds: the job
// definition plus the freshly-minted JobRun the worker now owns.
type ClaimResult struct {
	Job model.Job
	Run model.JobRun
}

// Claim atomically leases the highest-priority eligible pending job using
// SELECT … FOR UPDATE SKIP LOCKED, then inserts the JobRun for this
// attempt in the same transaction. Returns (result, true) on success,
// (_, false) when nothing is claimable.
func Claim(p ClaimParams) (ClaimResult, bool, error) {
	if len(p.Kinds) == 0 {
		return ClaimResult{}, false, nil
	}
	if p.WorkerID == "" {
		return ClaimResult{}, false, errors.New("worker_id is required")
	}
	if p.LeaseSec <= 0 {
		p.LeaseSec = config.DefaultLeaseSec
	}
	now := time.Now()
	lease := now.Add(time.Duration(p.LeaseSec) * time.Second)

	// The atomic claim. Note we increment attempt_count and flip status
	// here — but we do NOT write worker / lease / progress to the job
	// row; those live on the JobRun we create next.
	sql := `
		UPDATE jobs SET
			status        = 'active',
			attempt_count = attempt_count + 1,
			started_at    = COALESCE(started_at, now()),
			updated_at    = now()
		WHERE id = (
			SELECT id FROM jobs
			WHERE status = 'pending'
			  AND kind IN ?
			  AND (? OR queue IN ?)
			  AND (scheduled_at IS NULL OR scheduled_at <= now())
			ORDER BY priority DESC, enqueued_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING *;`

	noQueueFilter := len(p.Queues) == 0
	queues := p.Queues
	if noQueueFilter {
		queues = []string{""} // placeholder; guarded by the boolean
	}

	var out ClaimResult
	var found bool
	err := database.DB.Transaction(func(tx *gorm.DB) error {
		res := tx.Raw(sql, p.Kinds, noQueueFilter, queues).Scan(&out.Job)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 || out.Job.ID == "" {
			return nil
		}
		found = true
		out.Run = model.JobRun{
			ID:             ulid.Make().Prefixed("run"),
			JobID:          out.Job.ID,
			Attempt:        out.Job.AttemptCount,
			WorkerID:       p.WorkerID,
			Status:         model.RunStatusRunning,
			LeaseExpiresAt: &lease,
			StartedAt:      now,
		}
		return tx.Create(&out.Run).Error
	})
	if err != nil {
		return ClaimResult{}, false, err
	}
	return out, found, nil
}

// ---------- Heartbeat ----------

type ProgressUpdate struct {
	Current *int64
	Total   *int64
	Message *string
}

// Heartbeat extends the lease on the calling worker's in-flight run and
// optionally writes progress. Touches only job_runs — the parent job's
// shape doesn't change just because a worker pinged. Returns the updated
// run so the worker can confirm its lease.
func Heartbeat(jobID, workerID string, prog ProgressUpdate, leaseSec int) (model.JobRun, error) {
	if leaseSec <= 0 {
		leaseSec = config.DefaultLeaseSec
	}
	now := time.Now()
	lease := now.Add(time.Duration(leaseSec) * time.Second)

	updates := map[string]any{
		"lease_expires_at": lease,
		"updated_at":       now,
	}
	applyProgress(updates, prog)

	var run model.JobRun
	err := database.DB.Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&model.JobRun{}).
			Where("job_id = ? AND worker_id = ? AND status = ?",
				jobID, workerID, model.RunStatusRunning).
			Updates(updates)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return notRunningErr(jobID)
		}
		return tx.Where("job_id = ? AND worker_id = ? AND status = ?",
			jobID, workerID, model.RunStatusRunning).
			First(&run).Error
	})
	return run, err
}

// ---------- Complete ----------

// Complete closes out the calling worker's in-flight run as succeeded and
// terminalizes the parent job. The result is denormed onto Job.Result so
// list views can render outcomes without joining.
func Complete(jobID, workerID string, result model.JSON) (model.Job, error) {
	now := time.Now()
	var job model.Job
	err := database.DB.Transaction(func(tx *gorm.DB) error {
		runUpdates := map[string]any{
			"status":           model.RunStatusSucceeded,
			"finished_at":      now,
			"lease_expires_at": nil,
			"updated_at":       now,
		}
		if result != nil {
			runUpdates["result"] = result
		}
		res := tx.Model(&model.JobRun{}).
			Where("job_id = ? AND worker_id = ? AND status = ?",
				jobID, workerID, model.RunStatusRunning).
			Updates(runUpdates)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return notRunningErr(jobID)
		}

		jobUpdates := map[string]any{
			"status":       model.StatusSucceeded,
			"completed_at": now,
			"updated_at":   now,
		}
		if result != nil {
			jobUpdates["result"] = result
		}
		if err := tx.Model(&model.Job{}).Where("id = ?", jobID).Updates(jobUpdates).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", jobID).First(&job).Error
	})
	return job, err
}

// ---------- Fail ----------

// Fail closes out the calling worker's in-flight run as failed and
// decides whether the parent job retries (status -> pending with backoff)
// or terminalizes (status -> failed). The run row stays immutable
// history either way.
func Fail(jobID, workerID, errMsg string, retryable bool, backoff time.Duration) (model.Job, error) {
	now := time.Now()
	var job model.Job
	err := database.DB.Transaction(func(tx *gorm.DB) error {
		runRes := tx.Model(&model.JobRun{}).
			Where("job_id = ? AND worker_id = ? AND status = ?",
				jobID, workerID, model.RunStatusRunning).
			Updates(map[string]any{
				"status":           model.RunStatusFailed,
				"error":            errMsg,
				"finished_at":      now,
				"lease_expires_at": nil,
				"updated_at":       now,
			})
		if runRes.Error != nil {
			return runRes.Error
		}
		if runRes.RowsAffected == 0 {
			return notRunningErr(jobID)
		}

		// Lock the job to make the retry-vs-terminal decision atomic
		// with the run closeout.
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", jobID).First(&job).Error; err != nil {
			return err
		}

		jobUpdates := map[string]any{"updated_at": now}
		if retryable && job.AttemptCount < job.MaxAttempts {
			jobUpdates["status"] = model.StatusPending
			jobUpdates["scheduled_at"] = now.Add(backoff)
		} else {
			jobUpdates["status"] = model.StatusFailed
			jobUpdates["completed_at"] = now
		}
		if err := tx.Model(&model.Job{}).Where("id = ?", jobID).Updates(jobUpdates).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", jobID).First(&job).Error
	})
	return job, err
}

// ---------- Cancel ----------

// Cancel cancels a pending job immediately, or flags a running job for
// cooperative cancellation (the worker observes cancel_requested on its
// next heartbeat and stops via Fail). Terminal jobs are returned
// unchanged.
func Cancel(jobID string) (model.Job, error) {
	now := time.Now()
	var job model.Job
	err := database.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", jobID).First(&job).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNotFound
			}
			return err
		}
		switch job.Status {
		case model.StatusPending:
			if err := tx.Model(&model.Job{}).Where("id = ?", jobID).Updates(map[string]any{
				"status":       model.StatusCancelled,
				"completed_at": now,
				"updated_at":   now,
			}).Error; err != nil {
				return err
			}
		case model.StatusActive:
			if err := tx.Model(&model.Job{}).Where("id = ?", jobID).Updates(map[string]any{
				"cancel_requested": true,
				"updated_at":       now,
			}).Error; err != nil {
				return err
			}
		default:
			return nil
		}
		return tx.Where("id = ?", jobID).First(&job).Error
	})
	return job, err
}

// ---------- Reads ----------

func Get(id string) (model.Job, error) {
	var job model.Job
	if err := database.DB.Where("id = ?", id).First(&job).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.Job{}, ErrNotFound
		}
		return model.Job{}, err
	}
	return job, nil
}

// CurrentRun returns the in-flight run for a job, if any. The dashboard
// uses this to surface "what's happening right now" without bloating the
// Job row with denorm columns.
func CurrentRun(jobID string) (*model.JobRun, error) {
	var run model.JobRun
	err := database.DB.
		Where("job_id = ? AND status = ?", jobID, model.RunStatusRunning).
		First(&run).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &run, nil
}

// ListRuns returns every attempt at the given job, oldest first.
func ListRuns(jobID string) ([]model.JobRun, error) {
	var runs []model.JobRun
	err := database.DB.
		Where("job_id = ?", jobID).
		Order("attempt ASC").
		Find(&runs).Error
	return runs, err
}

type ListFilter struct {
	Status  string
	Kind    string
	Service string
	Queue   string
	Limit   int
	Cursor  string // job id; returns rows older than this (keyset on id desc)
}

func List(f ListFilter) ([]model.Job, error) {
	q := database.DB.Model(&model.Job{})
	if f.Status != "" {
		q = q.Where("status = ?", f.Status)
	}
	if f.Kind != "" {
		q = q.Where("kind = ?", f.Kind)
	}
	if f.Service != "" {
		q = q.Where("service = ?", f.Service)
	}
	if f.Queue != "" {
		q = q.Where("queue = ?", f.Queue)
	}
	if f.Cursor != "" {
		q = q.Where("id < ?", f.Cursor)
	}
	limit := f.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var jobs []model.Job
	if err := q.Order("id DESC").Limit(limit).Find(&jobs).Error; err != nil {
		return nil, err
	}
	return jobs, nil
}

// ---------- helpers ----------

func applyProgress(updates map[string]any, prog ProgressUpdate) {
	if prog.Current != nil {
		updates["progress_current"] = *prog.Current
	}
	if prog.Total != nil {
		updates["progress_total"] = *prog.Total
	}
	if prog.Message != nil {
		updates["progress_message"] = *prog.Message
	}
}

// notRunningErr disambiguates a zero-row mutation on the in-flight run:
// a missing parent job is ErrNotFound, an existing-but-not-owned (or
// already-terminal) run is ErrNotOwned.
func notRunningErr(id string) error {
	if _, err := Get(id); errors.Is(err, ErrNotFound) {
		return ErrNotFound
	}
	return ErrNotOwned
}
