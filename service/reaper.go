package service

import (
	"time"

	"github.com/gaucho-racing/foreman/config"
	"github.com/gaucho-racing/foreman/database"
	"github.com/gaucho-racing/foreman/pkg/logger"
)

// StartReaper sweeps in-flight runs whose lease has expired (the worker
// crashed or stalled). Each abandoned run flips its parent job back to
// pending if attempts remain, or terminalizes it as failed if exhausted.
func StartReaper() {
	interval := time.Duration(config.ReaperIntervalSec) * time.Second
	logger.SugarLogger.Infof("[REAPER] starting (tick=%s)", interval)
	go func() {
		for {
			if n, err := reapExpired(); err != nil {
				logger.SugarLogger.Errorf("[REAPER] sweep failed: %v", err)
			} else if n > 0 {
				logger.SugarLogger.Warnf("[REAPER] reclaimed %d expired lease(s)", n)
			}
			time.Sleep(interval)
		}
	}()
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
