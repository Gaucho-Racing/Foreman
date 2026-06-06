package database

import (
	"fmt"
	"strings"
	"time"

	"github.com/gaucho-racing/foreman/config"
	"github.com/gaucho-racing/foreman/model"
	"github.com/gaucho-racing/foreman/pkg/logger"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB

var dbRetries = 0

func Init() {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable TimeZone=UTC", config.DatabaseHost, config.DatabaseUser, config.DatabasePassword, config.DatabaseName, config.DatabasePort)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		if dbRetries < 5 {
			dbRetries++
			logger.SugarLogger.Errorln("failed to connect database, retrying in 5s... ")
			time.Sleep(time.Second * 5)
			Init()
		} else {
			logger.SugarLogger.Fatalf("failed to connect database after 5 attempts")
		}
	} else {
		logger.SugarLogger.Infoln("Connected to database")
		db.AutoMigrate(&model.Job{}, &model.JobRun{}, &model.Schedule{})
		if err := applySchemaExtensions(db); err != nil {
			logger.SugarLogger.Fatalf("schema extensions failed: %v", err)
		}
		logger.SugarLogger.Infoln("AutoMigration complete")
		DB = db
	}
}

// applySchemaExtensions layers things AutoMigrate can't express:
// foreign keys, CHECK constraints, partial indexes. All statements
// are safe to run on every boot — indexes use IF NOT EXISTS,
// constraints are wrapped in DO blocks that swallow duplicate_object.
//
// Order matters: drop the full-table indexes that the partial ones
// supersede first, so the rebuild doesn't briefly double-index.
func applySchemaExtensions(db *gorm.DB) error {
	stmts := []string{
		// --- Constraints ---
		// FK with cascade so deleting a job auto-deletes its runs.
		`DO $$ BEGIN
			ALTER TABLE job_runs
				ADD CONSTRAINT job_runs_job_id_fkey
				FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE;
		EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,

		// Defensive CHECK on status — catches typos in code before they
		// reach the row. Trivial to add now, painful to add later if
		// any stray value ever lands.
		`DO $$ BEGIN
			ALTER TABLE jobs
				ADD CONSTRAINT jobs_status_check
				CHECK (status IN ('pending','active','succeeded','failed','cancelled'));
		EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,

		`DO $$ BEGIN
			ALTER TABLE job_runs
				ADD CONSTRAINT job_runs_status_check
				CHECK (status IN ('running','succeeded','failed','abandoned'));
		EXCEPTION WHEN duplicate_object THEN NULL; END $$;`,

		// --- Drop superseded full-table indexes ---
		`DROP INDEX IF EXISTS idx_job_runs_lease_expires_at;`,
		`DROP INDEX IF EXISTS idx_schedules_next_fire_at;`,

		// --- Partial indexes for the hot paths ---
		// Claim filters status='pending' (vast minority in steady state)
		// and orders by priority + enqueued_at. Partial lets the scan
		// skip terminal rows entirely.
		`CREATE INDEX IF NOT EXISTS idx_jobs_pending_claim
			ON jobs (kind, scheduled_at, priority DESC, enqueued_at)
			WHERE status = 'pending';`,

		// Reaper scans job_runs.status='running' AND lease_expires_at <
		// now(). Partial covers only in-flight runs.
		`CREATE INDEX IF NOT EXISTS idx_job_runs_running_lease
			ON job_runs (lease_expires_at)
			WHERE status = 'running';`,

		// Scheduler's pluck is "enabled AND next_fire_at <= now()".
		// Partial on enabled — disabled schedules just don't appear.
		`CREATE INDEX IF NOT EXISTS idx_schedules_due
			ON schedules (next_fire_at)
			WHERE enabled;`,
	}
	for _, s := range stmts {
		if err := db.Exec(s).Error; err != nil {
			return fmt.Errorf("apply %q: %w", strings.SplitN(strings.TrimSpace(s), "\n", 2)[0], err)
		}
	}
	return nil
}
