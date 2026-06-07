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
//
// Constraint and index identifiers carry the same prefix as the tables
// they're attached to — Postgres constraint names are unique per
// schema, so unprefixed names would collide if two Foreman instances
// shared a database with different table prefixes.
func applySchemaExtensions(db *gorm.DB) error {
	jobs := model.TableJobs()
	runs := model.TableJobRuns()
	scheds := model.TableSchedules()
	stmts := []string{
		// --- Constraints ---
		// FK with cascade so deleting a job auto-deletes its runs.
		fmt.Sprintf(`DO $$ BEGIN
			ALTER TABLE %[1]s
				ADD CONSTRAINT %[1]s_job_id_fkey
				FOREIGN KEY (job_id) REFERENCES %[2]s(id) ON DELETE CASCADE;
		EXCEPTION WHEN duplicate_object THEN NULL; END $$;`, runs, jobs),

		// Defensive CHECK on status — catches typos in code before they
		// reach the row. Trivial to add now, painful to add later if
		// any stray value ever lands.
		fmt.Sprintf(`DO $$ BEGIN
			ALTER TABLE %[1]s
				ADD CONSTRAINT %[1]s_status_check
				CHECK (status IN ('pending','active','succeeded','failed','cancelled'));
		EXCEPTION WHEN duplicate_object THEN NULL; END $$;`, jobs),

		fmt.Sprintf(`DO $$ BEGIN
			ALTER TABLE %[1]s
				ADD CONSTRAINT %[1]s_status_check
				CHECK (status IN ('running','succeeded','failed','abandoned'));
		EXCEPTION WHEN duplicate_object THEN NULL; END $$;`, runs),

		// --- Drop superseded full-table indexes (legacy names) ---
		fmt.Sprintf(`DROP INDEX IF EXISTS idx_%s_lease_expires_at;`, runs),
		fmt.Sprintf(`DROP INDEX IF EXISTS idx_%s_next_fire_at;`, scheds),

		// --- Partial indexes for the hot paths ---
		// Claim filters status='pending' (vast minority in steady state)
		// and orders by priority + enqueued_at. Partial lets the scan
		// skip terminal rows entirely.
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%[1]s_pending_claim
			ON %[1]s (kind, scheduled_at, priority DESC, enqueued_at)
			WHERE status = 'pending';`, jobs),

		// Reaper scans job_runs.status='running' AND lease_expires_at <
		// now(). Partial covers only in-flight runs.
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%[1]s_running_lease
			ON %[1]s (lease_expires_at)
			WHERE status = 'running';`, runs),

		// Scheduler's pluck is "enabled AND next_fire_at <= now()".
		// Partial on enabled — disabled schedules just don't appear.
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%[1]s_due
			ON %[1]s (next_fire_at)
			WHERE enabled;`, scheds),
	}
	for _, s := range stmts {
		if err := db.Exec(s).Error; err != nil {
			return fmt.Errorf("apply %q: %w", strings.SplitN(strings.TrimSpace(s), "\n", 2)[0], err)
		}
	}
	return nil
}
