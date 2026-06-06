package service

import (
	"strings"
	"testing"
	"time"

	"github.com/gaucho-racing/foreman/database"
	"github.com/gaucho-racing/foreman/model"
)

// ---------- Validation ----------

func TestCreateSchedule_RejectsBadCron(t *testing.T) {
	resetDB(t)
	_, err := CreateSchedule(ScheduleParams{
		Kind: "k", CronExpr: "not a cron", Enabled: true,
	})
	if err == nil {
		t.Fatal("expected bad cron to reject")
	}
	if !strings.Contains(err.Error(), "cron_expr") {
		t.Fatalf("error should mention cron_expr: %v", err)
	}
}

func TestCreateSchedule_RejectsBadTimezone(t *testing.T) {
	resetDB(t)
	_, err := CreateSchedule(ScheduleParams{
		Kind: "k", CronExpr: "@hourly", Timezone: "Not/A_Zone", Enabled: true,
	})
	if err == nil || !strings.Contains(err.Error(), "timezone") {
		t.Fatalf("expected timezone error, got %v", err)
	}
}

func TestCreateSchedule_ComputesNextFireAtInFuture(t *testing.T) {
	resetDB(t)
	s, err := CreateSchedule(ScheduleParams{
		Kind: "k", CronExpr: "@every 1h", Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !s.NextFireAt.After(time.Now()) {
		t.Fatalf("NextFireAt should be in the future: %v", s.NextFireAt)
	}
}

// ---------- Tick ----------

// forceDueNow shoves the schedule's NextFireAt into the past so Tick
// will pick it up immediately, without waiting for cron's next slot.
func forceDueNow(t *testing.T, id string) {
	t.Helper()
	if err := database.DB.Model(&model.Schedule{}).
		Where("id = ?", id).
		Update("next_fire_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
}

func TestTick_FiresDueSchedule(t *testing.T) {
	resetDB(t)
	s, _ := CreateSchedule(ScheduleParams{
		Kind: "k", CronExpr: "@every 1h", Enabled: true, MaxAttempts: 1,
	})
	forceDueNow(t, s.ID)

	n, err := Tick()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("fired=%d, want 1", n)
	}

	// A job exists with the schedule's kind + the deterministic key.
	var jobs []model.Job
	if err := database.DB.Where("kind = ?", "k").Find(&jobs).Error; err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
	if jobs[0].IdempotencyKey == nil || !strings.HasPrefix(*jobs[0].IdempotencyKey, "sched:"+s.ID+":") {
		t.Fatalf("idempotency key mismatch: %v", jobs[0].IdempotencyKey)
	}

	// Schedule advanced: last_job_id matches, next_fire_at moved forward.
	updated, _ := GetSchedule(s.ID)
	if updated.LastJobID != jobs[0].ID {
		t.Fatalf("LastJobID=%s, want %s", updated.LastJobID, jobs[0].ID)
	}
	if !updated.NextFireAt.After(time.Now()) {
		t.Fatalf("NextFireAt should advance into the future: %v", updated.NextFireAt)
	}
}

// TestTick_IsIdempotentOnDuplicateFire simulates a scheduler crash by
// running fireOneInTx twice for the same fire instant. The second
// call should hit the idempotency key + advance past it rather than
// double-enqueue.
func TestTick_IdempotentOnRetry(t *testing.T) {
	resetDB(t)
	s, _ := CreateSchedule(ScheduleParams{
		Kind: "k", CronExpr: "@every 1h", Enabled: true, MaxAttempts: 1,
	})
	forceDueNow(t, s.ID)

	if _, err := Tick(); err != nil {
		t.Fatal(err)
	}
	// Force "due" again at the same intended fire time by rewinding
	// NextFireAt to the previous slot (last_fire_at).
	updated, _ := GetSchedule(s.ID)
	if updated.LastFireAt == nil {
		t.Fatalf("LastFireAt should be set after first fire")
	}
	if err := database.DB.Model(&model.Schedule{}).Where("id = ?", s.ID).
		Update("next_fire_at", *updated.LastFireAt).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := Tick(); err != nil {
		t.Fatal(err)
	}

	// Still exactly one job for this kind.
	var n int64
	database.DB.Model(&model.Job{}).Where("kind = ?", "k").Count(&n)
	if n != 1 {
		t.Fatalf("expected 1 job after duplicate tick, got %d", n)
	}
}

func TestTick_NoReplayOfMissedSlots(t *testing.T) {
	resetDB(t)
	s, _ := CreateSchedule(ScheduleParams{
		Kind: "k", CronExpr: "@every 10s", Enabled: true, MaxAttempts: 1,
	})
	// Pretend the scheduler was down for an hour by setting NextFireAt
	// way in the past. A naive "advance to next slot from fire_at"
	// would enqueue ~360 jobs; ours catches up with one.
	if err := database.DB.Model(&model.Schedule{}).Where("id = ?", s.ID).
		Update("next_fire_at", time.Now().Add(-1*time.Hour)).Error; err != nil {
		t.Fatal(err)
	}

	n, err := Tick()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("fired=%d, want 1 catch-up fire", n)
	}

	var jobs int64
	database.DB.Model(&model.Job{}).Where("kind = ?", "k").Count(&jobs)
	if jobs != 1 {
		t.Fatalf("enqueued %d jobs, want 1", jobs)
	}
}

// ---------- Manual fire ----------

func TestFireSchedule_DoesNotMoveNextFireAt(t *testing.T) {
	resetDB(t)
	s, _ := CreateSchedule(ScheduleParams{
		Kind: "k", CronExpr: "@every 1h", Enabled: true, MaxAttempts: 1,
	})

	before := s.NextFireAt
	if _, err := FireSchedule(s.ID); err != nil {
		t.Fatal(err)
	}

	after, _ := GetSchedule(s.ID)
	if !after.NextFireAt.Equal(before) {
		t.Fatalf("manual fire moved NextFireAt: %v -> %v", before, after.NextFireAt)
	}
	var jobs int64
	database.DB.Model(&model.Job{}).Where("kind = ?", "k").Count(&jobs)
	if jobs != 1 {
		t.Fatalf("manual fire enqueued %d jobs, want 1", jobs)
	}
}

// ---------- Update ----------

func TestUpdateSchedule_CronChangeRecomputesNextFireAt(t *testing.T) {
	resetDB(t)
	s, _ := CreateSchedule(ScheduleParams{
		Kind: "k", CronExpr: "@every 24h", Enabled: true, MaxAttempts: 1,
	})
	originalNext := s.NextFireAt

	// Tighten cron — next_fire_at should shift much closer.
	updated, err := UpdateSchedule(s.ID, ScheduleParams{
		Kind: "k", CronExpr: "@every 5m", Enabled: true, MaxAttempts: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !updated.NextFireAt.Before(originalNext) {
		t.Fatalf("expected NextFireAt to shift earlier: original=%v new=%v",
			originalNext, updated.NextFireAt)
	}
}
