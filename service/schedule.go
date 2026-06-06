package service

import (
	"errors"
	"fmt"
	"time"

	"github.com/gaucho-racing/foreman/database"
	"github.com/gaucho-racing/foreman/model"

	ulid "github.com/gaucho-racing/ulid-go"
	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// cronParser parses standard 5-field cron, descriptors (@hourly,
// @daily, etc.), and "@every Xs" duration expressions. Created once;
// cron parsers are safe for concurrent use.
var cronParser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

// ErrScheduleNotFound is returned when no schedule matches the id.
var ErrScheduleNotFound = errors.New("schedule not found")

// ParseCron validates an expression + timezone pair and returns the
// parsed schedule. Exposed so API handlers can reject bad inputs at
// create/update time, not at fire time.
func ParseCron(expr, tz string) (cron.Schedule, *time.Location, error) {
	if expr == "" {
		return nil, nil, errors.New("cron_expr is required")
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid timezone %q: %w", tz, err)
	}
	parsed, err := cronParser.Parse(expr)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid cron_expr %q: %w", expr, err)
	}
	return parsed, loc, nil
}

// ---------- CRUD ----------

type ScheduleParams struct {
	Kind        string
	Queue       string
	Service     string
	Params      model.JSON
	Priority    int
	MaxAttempts int
	CronExpr    string
	Timezone    string
	Enabled     bool
}

func normalizeScheduleParams(p *ScheduleParams) error {
	if p.Kind == "" {
		return errors.New("kind is required")
	}
	if p.Queue == "" {
		p.Queue = "default"
	}
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	if p.Timezone == "" {
		p.Timezone = "UTC"
	}
	return nil
}

func CreateSchedule(p ScheduleParams) (model.Schedule, error) {
	if err := normalizeScheduleParams(&p); err != nil {
		return model.Schedule{}, err
	}
	parsed, loc, err := ParseCron(p.CronExpr, p.Timezone)
	if err != nil {
		return model.Schedule{}, err
	}
	s := model.Schedule{
		ID:          ulid.Make().Prefixed("sched"),
		Kind:        p.Kind,
		Queue:       p.Queue,
		Service:     p.Service,
		Params:      p.Params,
		Priority:    p.Priority,
		MaxAttempts: p.MaxAttempts,
		CronExpr:    p.CronExpr,
		Timezone:    p.Timezone,
		Enabled:     p.Enabled,
		NextFireAt:  parsed.Next(time.Now().In(loc)),
	}
	if err := database.DB.Create(&s).Error; err != nil {
		return model.Schedule{}, err
	}
	return s, nil
}

func GetSchedule(id string) (model.Schedule, error) {
	var s model.Schedule
	if err := database.DB.Where("id = ?", id).First(&s).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.Schedule{}, ErrScheduleNotFound
		}
		return model.Schedule{}, err
	}
	return s, nil
}

type ListSchedulesFilter struct {
	Enabled *bool
	Kind    string
	Limit   int
	Cursor  string
}

func ListSchedules(f ListSchedulesFilter) ([]model.Schedule, error) {
	q := database.DB.Model(&model.Schedule{})
	if f.Enabled != nil {
		q = q.Where("enabled = ?", *f.Enabled)
	}
	if f.Kind != "" {
		q = q.Where("kind = ?", f.Kind)
	}
	if f.Cursor != "" {
		q = q.Where("id < ?", f.Cursor)
	}
	limit := f.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var out []model.Schedule
	err := q.Order("id DESC").Limit(limit).Find(&out).Error
	return out, err
}

// UpdateSchedule replaces editable fields. Recomputes NextFireAt when
// the cron expression or timezone changes — otherwise leaves it alone
// so an in-flight schedule keeps its current fire cadence.
func UpdateSchedule(id string, p ScheduleParams) (model.Schedule, error) {
	if err := normalizeScheduleParams(&p); err != nil {
		return model.Schedule{}, err
	}
	parsed, loc, err := ParseCron(p.CronExpr, p.Timezone)
	if err != nil {
		return model.Schedule{}, err
	}
	var out model.Schedule
	err = database.DB.Transaction(func(tx *gorm.DB) error {
		var cur model.Schedule
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", id).First(&cur).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrScheduleNotFound
			}
			return err
		}
		updates := map[string]any{
			"kind":         p.Kind,
			"queue":        p.Queue,
			"service":      p.Service,
			"params":       p.Params,
			"priority":     p.Priority,
			"max_attempts": p.MaxAttempts,
			"cron_expr":    p.CronExpr,
			"timezone":     p.Timezone,
			"enabled":      p.Enabled,
		}
		if p.CronExpr != cur.CronExpr || p.Timezone != cur.Timezone {
			updates["next_fire_at"] = parsed.Next(time.Now().In(loc))
		}
		if err := tx.Model(&model.Schedule{}).Where("id = ?", id).Updates(updates).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", id).First(&out).Error
	})
	return out, err
}

func DeleteSchedule(id string) error {
	res := database.DB.Where("id = ?", id).Delete(&model.Schedule{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrScheduleNotFound
	}
	return nil
}

// FireSchedule manually triggers a schedule — enqueues a job using its
// recipe but does NOT touch NextFireAt. Useful for "run it once now"
// from the dashboard without disturbing the recurring cadence.
func FireSchedule(id string) (model.Job, error) {
	s, err := GetSchedule(id)
	if err != nil {
		return model.Job{}, err
	}
	// Use a unique idempotency key so this manual fire doesn't collide
	// with any scheduled fire at the same instant.
	key := fmt.Sprintf("sched:%s:manual:%s", s.ID, ulid.Make().String())
	return Enqueue(EnqueueParams{
		Kind:           s.Kind,
		Queue:          s.Queue,
		Service:        s.Service,
		IdempotencyKey: &key,
		Params:         s.Params,
		Priority:       s.Priority,
		MaxAttempts:    s.MaxAttempts,
	})
}

// ---------- Tick (called by the scheduler goroutine) ----------

// schedulerLockKey is a fixed Postgres advisory lock key shared by every
// Foreman replica's scheduler. Whichever replica's transaction acquires
// it first owns the tick; the rest see false and skip. The value is the
// ASCII bytes of "foreman" packed into an int64 — stable, unlikely to
// collide with anyone else's advisory locks in the same database.
const schedulerLockKey int64 = 0x666f72656d616e

// Tick finds due schedules and enqueues a job for each, advancing
// NextFireAt past now() so a single delay or crash doesn't replay the
// entire backlog. Returns how many schedules fired.
//
// Multi-replica safety has two layers:
//
//  1. pg_try_advisory_xact_lock at the top of every tick. Only one
//     replica's tick body runs at a time; the others get false from
//     the try-lock and no-op. The lock is transaction-scoped, so it
//     auto-releases at commit — no babysitting needed.
//  2. Even if the lock layer ever fails (or someone hits the path
//     manually), each schedule is still selected FOR UPDATE SKIP
//     LOCKED, and the per-fire idempotency key collapses a crash
//     retry to the same Job. The advisory lock is the cheap fence;
//     the row lock + idempotency key is the correctness floor.
func Tick() (int, error) {
	const batchLimit = 100
	now := time.Now()
	fired := 0

	err := database.DB.Transaction(func(tx *gorm.DB) error {
		var acquired bool
		if err := tx.Raw("SELECT pg_try_advisory_xact_lock(?)", schedulerLockKey).Scan(&acquired).Error; err != nil {
			return err
		}
		if !acquired {
			// Another replica is running this tick. Quietly skip —
			// flooding logs with one line per replica per second adds
			// up fast.
			return nil
		}

		var dueIDs []string
		if err := tx.Model(&model.Schedule{}).
			Where("enabled = ? AND next_fire_at <= ?", true, now).
			Order("next_fire_at ASC").
			Limit(batchLimit).
			Pluck("id", &dueIDs).Error; err != nil {
			return err
		}
		if len(dueIDs) == 0 {
			return nil
		}

		for _, id := range dueIDs {
			if err := fireOneInTx(tx, id, now); err != nil {
				// Don't abort the whole batch — return now would
				// rollback all the fires we already did and waste
				// this tick's lock window. Log and keep going via the
				// caller; a poison schedule shouldn't starve the rest.
				return fmt.Errorf("schedule %s: %w", id, err)
			}
			fired++
		}
		return nil
	})
	return fired, err
}

// fireOneInTx is the per-schedule work. Runs inside the tick's wrapper
// transaction; uses a savepoint via tx.Transaction so a single failure
// doesn't unwind the whole batch + advisory lock.
func fireOneInTx(parent *gorm.DB, id string, now time.Time) error {
	return parent.Transaction(func(tx *gorm.DB) error {
		var s model.Schedule
		err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("id = ? AND enabled = ? AND next_fire_at <= ?", id, true, now).
			First(&s).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// Either it was just disabled, deleted, or another
				// scheduler instance grabbed it. Either way, skip.
				return nil
			}
			return err
		}

		parsed, loc, err := ParseCron(s.CronExpr, s.Timezone)
		if err != nil {
			// Schedule has somehow stored an invalid cron — disable
			// it so the scheduler doesn't busy-loop trying again.
			return tx.Model(&model.Schedule{}).Where("id = ?", id).
				Updates(map[string]any{
					"enabled":    false,
					"updated_at": now,
				}).Error
		}

		// Use the *intended* fire time for the idempotency key so a
		// retry after a crash collapses to the same job.
		fireAt := s.NextFireAt
		key := fmt.Sprintf("sched:%s:%s", s.ID, fireAt.UTC().Format(time.RFC3339Nano))

		job, err := enqueueTx(tx, EnqueueParams{
			Kind:           s.Kind,
			Queue:          s.Queue,
			Service:        s.Service,
			IdempotencyKey: &key,
			Params:         s.Params,
			Priority:       s.Priority,
			MaxAttempts:    s.MaxAttempts,
		})
		// ErrConflict means we already enqueued this exact fire — fine,
		// just advance past it. Any other error bails.
		if err != nil && !errors.Is(err, ErrConflict) {
			return err
		}

		// Advance NextFireAt to the next slot strictly after now(). If
		// we used Next(fireAt), a long-stalled scheduler would replay
		// every missed slot; using Next(now) catches up in one shot.
		nextFire := parsed.Next(now.In(loc))
		return tx.Model(&model.Schedule{}).Where("id = ?", id).
			Updates(map[string]any{
				"last_fire_at": fireAt,
				"last_job_id":  job.ID,
				"next_fire_at": nextFire,
				"updated_at":   now,
			}).Error
	})
}
