package service

import (
	"errors"
	"time"

	"github.com/gaucho-racing/foreman/config"
	"github.com/gaucho-racing/foreman/database"
	"github.com/gaucho-racing/foreman/model"
	"github.com/gaucho-racing/foreman/pkg/metrics"

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
	job, err := enqueueTx(database.DB, p)
	switch {
	case err == nil:
		metrics.JobsEnqueued.WithLabelValues(job.Kind, job.Queue, "created").Inc()
	case errors.Is(err, ErrConflict):
		// `job` here is the *existing* row returned alongside ErrConflict.
		metrics.JobsEnqueued.WithLabelValues(job.Kind, job.Queue, "deduped").Inc()
	}
	return job, err
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
	if found {
		metrics.JobsClaimed.WithLabelValues(out.Job.Kind, out.Job.Queue).Inc()
		// First-attempt claim wait = time from enqueue to first claim.
		// COALESCE preserves started_at across retries, so attempts > 1
		// already had a "first claim time" recorded — don't observe.
		if out.Job.AttemptCount == 1 && out.Job.StartedAt != nil {
			wait := out.Job.StartedAt.Sub(out.Job.EnqueuedAt).Seconds()
			if wait >= 0 {
				metrics.ClaimWait.WithLabelValues(out.Job.Kind).Observe(wait)
			}
		}
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
// optionally writes progress. Identifies the run by id (not job id) —
// workers received the run id from Claim and use it for every
// lease-scoped mutation. Touches only job_runs.
func Heartbeat(runID, workerID string, prog ProgressUpdate, leaseSec int) (model.JobRun, error) {
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
			Where("id = ? AND worker_id = ? AND status = ?",
				runID, workerID, model.RunStatusRunning).
			Updates(updates)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return notRunningRunErr(runID)
		}
		return tx.Where("id = ?", runID).First(&run).Error
	})
	if err == nil {
		// Kind isn't on the run row; load via the parent job. Cheap
		// PK lookup, only on the success path.
		if job, gerr := Get(run.JobID); gerr == nil {
			metrics.Heartbeats.WithLabelValues(job.Kind).Inc()
		}
	}
	return run, err
}

// ---------- Complete ----------

// Complete closes out the calling worker's in-flight run as succeeded
// and terminalizes the parent job. The run id is the addressable handle
// (workers got it from Claim); the parent job_id is read off the run row
// so callers don't have to pass it twice. The result is denormed onto
// Job.Result so list views can render outcomes without joining.
func Complete(runID, workerID string, result model.JSON) (model.Job, error) {
	now := time.Now()
	var job model.Job
	var run model.JobRun
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
			Where("id = ? AND worker_id = ? AND status = ?",
				runID, workerID, model.RunStatusRunning).
			Updates(runUpdates)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return notRunningRunErr(runID)
		}

		// The just-updated run row carries the parent job_id; read it
		// back so we don't have to pass it through the API.
		if err := tx.Where("id = ?", runID).First(&run).Error; err != nil {
			return err
		}

		jobUpdates := map[string]any{
			"status":       model.StatusSucceeded,
			"completed_at": now,
			"updated_at":   now,
		}
		if result != nil {
			jobUpdates["result"] = result
		}
		if err := tx.Model(&model.Job{}).Where("id = ?", run.JobID).Updates(jobUpdates).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", run.JobID).First(&job).Error
	})
	if err == nil {
		metrics.JobsTerminal.WithLabelValues(job.Kind, model.StatusSucceeded).Inc()
		metrics.RunsTerminal.WithLabelValues(job.Kind, model.RunStatusSucceeded).Inc()
		observeJobLifetime(job)
		observeRunDuration(job.Kind, model.RunStatusSucceeded, run.StartedAt, now)
	}
	return job, err
}

// ---------- Fail ----------

// Fail closes out the calling worker's in-flight run as failed and
// decides whether the parent job retries (status -> pending with backoff)
// or terminalizes (status -> failed). An optional result payload is
// preserved on the run alongside the error — useful for failures that
// still want to surface partial data (e.g. "processed 50/100, then
// these specific records errored"). The run row stays immutable history
// either way; Job.result is *not* touched on Fail (it remains the
// winning run's result, if any).
func Fail(runID, workerID, errMsg string, retryable bool, backoff time.Duration, result model.JSON) (model.Job, error) {
	now := time.Now()
	var job model.Job
	var run model.JobRun
	err := database.DB.Transaction(func(tx *gorm.DB) error {
		runUpdates := map[string]any{
			"status":           model.RunStatusFailed,
			"error":            errMsg,
			"finished_at":      now,
			"lease_expires_at": nil,
			"updated_at":       now,
		}
		if result != nil {
			runUpdates["result"] = result
		}
		runRes := tx.Model(&model.JobRun{}).
			Where("id = ? AND worker_id = ? AND status = ?",
				runID, workerID, model.RunStatusRunning).
			Updates(runUpdates)
		if runRes.Error != nil {
			return runRes.Error
		}
		if runRes.RowsAffected == 0 {
			return notRunningRunErr(runID)
		}

		if err := tx.Where("id = ?", runID).First(&run).Error; err != nil {
			return err
		}

		// Lock the job to make the retry-vs-terminal decision atomic
		// with the run closeout.
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", run.JobID).First(&job).Error; err != nil {
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
		if err := tx.Model(&model.Job{}).Where("id = ?", run.JobID).Updates(jobUpdates).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", run.JobID).First(&job).Error
	})
	if err == nil {
		// Every Fail closes a run; record that.
		metrics.RunsTerminal.WithLabelValues(job.Kind, model.RunStatusFailed).Inc()
		observeRunDuration(job.Kind, model.RunStatusFailed, run.StartedAt, now)
		// Job-level terminal only fires when the parent flipped to a
		// terminal status — i.e. retries were exhausted or non-retryable.
		if job.IsTerminal() {
			metrics.JobsTerminal.WithLabelValues(job.Kind, model.StatusFailed).Inc()
			observeJobLifetime(job)
		}
	}
	return job, err
}

// ---------- Cancel ----------

// Cancel cancels a pending job immediately, or flags a running job for
// cooperative cancellation (the worker observes cancel_requested on its
// next heartbeat and stops via Fail). Terminal jobs are returned
// unchanged.
// observeJobLifetime records enqueued_at → completed_at for a terminal
// job. Skips cleanly when CompletedAt is somehow unset.
func observeJobLifetime(job model.Job) {
	if job.CompletedAt == nil {
		return
	}
	d := job.CompletedAt.Sub(job.EnqueuedAt).Seconds()
	if d >= 0 {
		metrics.JobLifetime.WithLabelValues(job.Kind, job.Status).Observe(d)
	}
}

// observeRunDuration records started_at → now (or finished_at) for a
// terminal run. Caller supplies the terminal status label so the same
// helper covers succeeded / failed / abandoned.
func observeRunDuration(kind, status string, startedAt, finishedAt time.Time) {
	d := finishedAt.Sub(startedAt).Seconds()
	if d >= 0 {
		metrics.RunDuration.WithLabelValues(kind, status).Observe(d)
	}
}

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
	// Only count "actually cancelled" transitions, not cooperative
	// requests on still-running jobs (those land as Fail later) and
	// not no-ops on already-terminal jobs.
	if err == nil && job.Status == model.StatusCancelled && job.CompletedAt != nil &&
		job.CompletedAt.Equal(now) {
		metrics.JobsTerminal.WithLabelValues(job.Kind, model.StatusCancelled).Inc()
		observeJobLifetime(job)
	}
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

// notRunningErr disambiguates a zero-row mutation when the caller
// addressed by job id: a missing job is ErrNotFound, anything else
// (cancelled/terminalized/wrong worker) is ErrNotOwned.
func notRunningErr(id string) error {
	if _, err := Get(id); errors.Is(err, ErrNotFound) {
		return ErrNotFound
	}
	return ErrNotOwned
}

// notRunningRunErr is the run-addressed equivalent. A missing run id is
// ErrNotFound; an existing-but-not-owned (terminal/wrong worker) run is
// ErrNotOwned.
func notRunningRunErr(runID string) error {
	var r model.JobRun
	if err := database.DB.Where("id = ?", runID).First(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrNotFound
		}
		return err
	}
	return ErrNotOwned
}

// GetRun returns a single run by id.
func GetRun(runID string) (model.JobRun, error) {
	var run model.JobRun
	if err := database.DB.Where("id = ?", runID).First(&run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.JobRun{}, ErrNotFound
		}
		return model.JobRun{}, err
	}
	return run, nil
}

// ListRunsFilter is the query shape for GET /foreman/runs — a global
// view across all jobs. Filter by run-level fields (status, worker, the
// owning job id) plus kind via a join to jobs.
type ListRunsFilter struct {
	Status   string
	WorkerID string
	JobID    string
	Kind     string
	Limit    int
	Cursor   string // run id; returns rows older than this (keyset on id desc)
}

// ListAllRuns returns runs across the whole queue with optional filters.
// Joins to jobs only when Kind is set — most callers won't need it.
func ListAllRuns(f ListRunsFilter) ([]model.JobRun, error) {
	q := database.DB.Model(&model.JobRun{})
	if f.Status != "" {
		q = q.Where("job_runs.status = ?", f.Status)
	}
	if f.WorkerID != "" {
		q = q.Where("worker_id = ?", f.WorkerID)
	}
	if f.JobID != "" {
		q = q.Where("job_id = ?", f.JobID)
	}
	if f.Kind != "" {
		q = q.Joins("JOIN jobs ON jobs.id = job_runs.job_id").
			Where("jobs.kind = ?", f.Kind)
	}
	if f.Cursor != "" {
		q = q.Where("job_runs.id < ?", f.Cursor)
	}
	limit := f.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var runs []model.JobRun
	err := q.Order("job_runs.id DESC").Limit(limit).Find(&runs).Error
	return runs, err
}

// CurrentRunsForJobs batches CurrentRun across many jobs in a single
// query. Returns a map keyed by job_id; missing jobs simply have no
// entry. Used by the jobs-list ?include=current_run expansion.
func CurrentRunsForJobs(jobIDs []string) (map[string]model.JobRun, error) {
	out := make(map[string]model.JobRun, len(jobIDs))
	if len(jobIDs) == 0 {
		return out, nil
	}
	var runs []model.JobRun
	err := database.DB.
		Where("job_id IN ? AND status = ?", jobIDs, model.RunStatusRunning).
		Find(&runs).Error
	if err != nil {
		return nil, err
	}
	for _, r := range runs {
		out[r.JobID] = r
	}
	return out, nil
}
