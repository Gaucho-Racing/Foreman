package service

import (
	"testing"
	"time"

	"github.com/gaucho-racing/foreman/database"
	"github.com/gaucho-racing/foreman/model"
)

// TestReaper_AbandonsExpiredAndBouncesRetryable hand-rolls an expired
// lease (sets lease_expires_at in the past) and runs reapExpired().
// The run should flip to abandoned and the parent job should bounce
// back to pending since attempt_count < max_attempts.
func TestReaper_AbandonsExpiredAndBouncesRetryable(t *testing.T) {
	resetDB(t)
	_ = mustEnqueue(t, "k", withMaxAttempts(3))
	res, _, _ := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-1", LeaseSec: 30})

	// Forge an expired lease without waiting 30s.
	if err := database.DB.Model(&model.JobRun{}).
		Where("id = ?", res.Run.ID).
		Update("lease_expires_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := reapExpired(); err != nil {
		t.Fatal(err)
	}

	// Run is abandoned + lease cleared.
	var run model.JobRun
	if err := database.DB.Where("id = ?", res.Run.ID).First(&run).Error; err != nil {
		t.Fatal(err)
	}
	if run.Status != model.RunStatusAbandoned {
		t.Fatalf("run.Status=%s, want abandoned", run.Status)
	}
	if run.LeaseExpiresAt != nil {
		t.Fatalf("run.LeaseExpiresAt should be NULL after reap, got %v", run.LeaseExpiresAt)
	}
	if run.Error == "" {
		t.Fatalf("run.Error should be set after reap")
	}

	// Job back to pending — attempts remained.
	job, _ := Get(res.Job.ID)
	if job.Status != model.StatusPending {
		t.Fatalf("job.Status=%s, want pending", job.Status)
	}
	if job.ScheduledAt == nil {
		t.Fatalf("job.ScheduledAt should be set after reap so claim re-sees it")
	}
}

func TestReaper_TerminalizesWhenExhausted(t *testing.T) {
	resetDB(t)
	_ = mustEnqueue(t, "k", withMaxAttempts(1))
	res, _, _ := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-1", LeaseSec: 30})

	database.DB.Model(&model.JobRun{}).
		Where("id = ?", res.Run.ID).
		Update("lease_expires_at", time.Now().Add(-time.Second))

	if _, err := reapExpired(); err != nil {
		t.Fatal(err)
	}
	job, _ := Get(res.Job.ID)
	if job.Status != model.StatusFailed {
		t.Fatalf("attempt_count==max_attempts should fail terminally: status=%s", job.Status)
	}
	if job.CompletedAt == nil {
		t.Fatalf("terminal job should have completed_at")
	}
}

func TestReaper_LeavesFreshLeasesAlone(t *testing.T) {
	resetDB(t)
	_ = mustEnqueue(t, "k")
	res, _, _ := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-1", LeaseSec: 60})

	if _, err := reapExpired(); err != nil {
		t.Fatal(err)
	}

	job, _ := Get(res.Job.ID)
	if job.Status != model.StatusActive {
		t.Fatalf("fresh lease should not be reaped: status=%s", job.Status)
	}
}

func TestRetention_PrunesOldTerminalsAndCascadesRuns(t *testing.T) {
	resetDB(t)
	// One terminal job past the cutoff…
	job := mustEnqueue(t, "k")
	res, _, _ := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-1", LeaseSec: 30})
	if _, err := Complete(res.Run.ID, "w-1", nil); err != nil {
		t.Fatal(err)
	}
	// Backdate completed_at past the retention cutoff (2 days).
	database.DB.Model(&model.Job{}).Where("id = ?", job.ID).
		Update("completed_at", time.Now().AddDate(0, 0, -3))

	// …and one terminal job inside the window.
	keep := mustEnqueue(t, "k")
	res2, _, _ := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-2", LeaseSec: 30})
	if _, err := Complete(res2.Run.ID, "w-2", nil); err != nil {
		t.Fatal(err)
	}

	n, err := pruneOldJobs(2)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("pruned=%d, want 1", n)
	}

	// Old job + its runs gone via CASCADE.
	var oldCount, oldRuns int64
	database.DB.Model(&model.Job{}).Where("id = ?", job.ID).Count(&oldCount)
	database.DB.Model(&model.JobRun{}).Where("job_id = ?", job.ID).Count(&oldRuns)
	if oldCount != 0 || oldRuns != 0 {
		t.Fatalf("old job not fully pruned: job_rows=%d run_rows=%d", oldCount, oldRuns)
	}

	// Recent job + run still there.
	var keepCount int64
	database.DB.Model(&model.Job{}).Where("id = ?", keep.ID).Count(&keepCount)
	if keepCount != 1 {
		t.Fatalf("recent job pruned by mistake")
	}
}

func TestRetention_LeavesNonTerminalJobsAlone(t *testing.T) {
	resetDB(t)
	// Pending job, ancient.
	job := mustEnqueue(t, "k")
	database.DB.Model(&model.Job{}).Where("id = ?", job.ID).
		Update("enqueued_at", time.Now().AddDate(0, 0, -90))

	n, _ := pruneOldJobs(1)
	if n != 0 {
		t.Fatalf("pending job should never be pruned, but n=%d", n)
	}
}
