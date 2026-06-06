package service

import (
	"time"

	"github.com/gaucho-racing/foreman/config"
	"github.com/gaucho-racing/foreman/database"
	"github.com/gaucho-racing/foreman/pkg/logger"

	"gorm.io/gorm"
)

// StartReaper sweeps running jobs whose lease has expired (the worker
// crashed or stalled). Jobs with attempts remaining return to pending for
// re-claim; exhausted ones are failed terminally. In both cases the
// in-flight JobRun is closed out as status=abandoned.
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

// reapExpired runs both the job-side reset and the run-side abandonment
// in a single transaction so observers never see an expired job in
// 'running' state without a closed-out run, or vice versa. Both
// statements use the same predicate (status='running' AND lease expired)
// so they target the exact same rows.
func reapExpired() (int64, error) {
	var affected int64
	err := database.DB.Transaction(func(tx *gorm.DB) error {
		// Job side: bounce to pending (retryable) or fail terminally.
		jobSQL := `
			UPDATE jobs SET
				status       = CASE WHEN attempt < max_attempts THEN 'pending' ELSE 'failed' END,
				worker_id    = CASE WHEN attempt < max_attempts THEN '' ELSE worker_id END,
				scheduled_at = CASE WHEN attempt < max_attempts THEN now() ELSE scheduled_at END,
				finished_at  = CASE WHEN attempt < max_attempts THEN NULL ELSE now() END,
				error        = CASE WHEN attempt < max_attempts THEN error ELSE 'lease expired' END,
				lease_expires_at = NULL,
				updated_at  = now()
			WHERE status = 'running'
			  AND lease_expires_at IS NOT NULL
			  AND lease_expires_at < now();`
		res := tx.Exec(jobSQL)
		if res.Error != nil {
			return res.Error
		}
		affected = res.RowsAffected

		// Run side: close out the in-flight run as abandoned. Same
		// predicate, applied to job_runs.
		runSQL := `
			UPDATE job_runs SET
				status      = 'abandoned',
				finished_at = now(),
				lease_expires_at = NULL,
				error       = COALESCE(NULLIF(error, ''), 'lease expired'),
				updated_at  = now()
			WHERE status = 'running'
			  AND lease_expires_at IS NOT NULL
			  AND lease_expires_at < now();`
		return tx.Exec(runSQL).Error
	})
	return affected, err
}
