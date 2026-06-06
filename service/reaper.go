package service

import (
	"math/rand/v2"
	"time"

	"github.com/gaucho-racing/foreman/config"
	"github.com/gaucho-racing/foreman/database"
	"github.com/gaucho-racing/foreman/pkg/logger"
	"github.com/gaucho-racing/foreman/pkg/metrics"
)

// StartReaper sweeps in-flight runs whose lease has expired (the worker
// crashed or stalled). Each abandoned run flips its parent job back to
// pending if attempts remain, or terminalizes it as failed if exhausted.
//
// Also opportunistically prunes terminal jobs older than
// FOREMAN_RETENTION_DAYS (when set). Retention runs every tick — it's
// a cheap DELETE, and the FK CASCADE on job_runs.job_id sweeps the
// run history alongside the parent job in the same statement.
//
// Replicas that boot together (e.g. during a rolling deploy) would
// otherwise tick in lockstep, multiplying redundant CTE attempts by N.
// A random 0..interval offset before the first sweep spreads them
// across the cycle; after that each instance runs on its own cadence.
func StartReaper() {
	interval := time.Duration(config.ReaperIntervalSec) * time.Second
	jitter := time.Duration(rand.Int64N(int64(interval)))
	logger.SugarLogger.Infof("[REAPER] starting (tick=%s, initial jitter=%s, retention_days=%d)",
		interval, jitter, config.RetentionDays)
	go func() {
		time.Sleep(jitter)
		for {
			if n, err := reapExpired(); err != nil {
				logger.SugarLogger.Errorf("[REAPER] sweep failed: %v", err)
				metrics.ReaperSweeps.WithLabelValues("error").Inc()
			} else {
				metrics.ReaperSweeps.WithLabelValues("success").Inc()
				if n > 0 {
					logger.SugarLogger.Warnf("[REAPER] reclaimed %d expired lease(s)", n)
					metrics.ReaperReaped.Add(float64(n))
				}
			}
			if config.RetentionDays > 0 {
				if n, err := pruneOldJobs(config.RetentionDays); err != nil {
					logger.SugarLogger.Errorf("[REAPER] retention sweep failed: %v", err)
				} else if n > 0 {
					logger.SugarLogger.Infof("[REAPER] retention deleted %d terminal job(s)", n)
					metrics.RetentionDeleted.Add(float64(n))
				}
			}
			time.Sleep(interval)
		}
	}()
}

// pruneOldJobs deletes terminal jobs older than `days` days. Schedules
// and in-flight jobs are untouched — only completed history goes.
// FK CASCADE on job_runs.job_id auto-deletes corresponding runs.
// Uses make_interval(days => ?) instead of (? || ' days')::interval so
// pgx can bind the parameter as an int — the text-concat form forces a
// text encode of `days` that pgx refuses to plan automatically.
func pruneOldJobs(days int) (int64, error) {
	sql := `
		DELETE FROM jobs
		WHERE status IN ('succeeded','failed','cancelled')
		  AND completed_at IS NOT NULL
		  AND completed_at < now() - make_interval(days => ?);`
	res := database.DB.Exec(sql, days)
	return res.RowsAffected, res.Error
}

// reapExpired runs as a single CTE: the inner UPDATE abandons every
// expired in-flight run, RETURNING the job_ids; the outer UPDATE bounces
// or terminalizes those jobs. One round-trip, atomic relative to other
// writers, and the predicate `status='running' AND lease_expires_at <
// now()` is shared so the same rows are touched on both sides.
func reapExpired() (int64, error) {
	sql := `
		WITH expired AS (
			UPDATE job_runs SET
				status           = 'abandoned',
				finished_at      = now(),
				error            = COALESCE(NULLIF(error, ''), 'lease expired'),
				lease_expires_at = NULL,
				updated_at       = now()
			WHERE status = 'running'
			  AND lease_expires_at IS NOT NULL
			  AND lease_expires_at < now()
			RETURNING job_id
		)
		UPDATE jobs SET
			status       = CASE WHEN attempt_count < max_attempts THEN 'pending' ELSE 'failed' END,
			scheduled_at = CASE WHEN attempt_count < max_attempts THEN now() ELSE scheduled_at END,
			completed_at = CASE WHEN attempt_count >= max_attempts THEN now() ELSE completed_at END,
			updated_at   = now()
		WHERE id IN (SELECT job_id FROM expired);`
	res := database.DB.Exec(sql)
	return res.RowsAffected, res.Error
}
