package service

import (
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gaucho-racing/foreman/database"
	"github.com/gaucho-racing/foreman/model"
)

// jsonEqual decodes both sides and compares structurally — jsonb
// normalizes whitespace on storage, so a byte-level comparison would
// flag `{"ok":true}` ≠ `{"ok": true}`. Skips that noise.
func jsonEqual(t *testing.T, got, want []byte) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("decode got: %v (raw=%s)", err, got)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("decode want: %v (raw=%s)", err, want)
	}
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("json mismatch:\n got=%s\nwant=%s", got, want)
	}
}

// ---------- Enqueue ----------

func TestEnqueue_NewJobIsPending(t *testing.T) {
	resetDB(t)
	job := mustEnqueue(t, "k")
	if job.Status != model.StatusPending {
		t.Fatalf("status=%q, want pending", job.Status)
	}
	if job.AttemptCount != 0 {
		t.Fatalf("attempt_count=%d, want 0", job.AttemptCount)
	}
}

func TestEnqueue_IdempotencyConflictReturnsExisting(t *testing.T) {
	resetDB(t)
	first := mustEnqueue(t, "k", withIdempotencyKey("dup"))

	_, err := Enqueue(EnqueueParams{
		Kind: "k", IdempotencyKey: ptr("dup"), MaxAttempts: 1,
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err=%v, want ErrConflict", err)
	}
	// The conflict returns the existing job — same id.
	existing, err2 := Enqueue(EnqueueParams{
		Kind: "k", IdempotencyKey: ptr("dup"), MaxAttempts: 1,
	})
	if !errors.Is(err2, ErrConflict) || existing.ID != first.ID {
		t.Fatalf("conflict job id mismatch: got %q, want %q", existing.ID, first.ID)
	}
}

func TestEnqueue_NilIdempotencyKeyAllowsDuplicates(t *testing.T) {
	resetDB(t)
	a := mustEnqueue(t, "k")
	b := mustEnqueue(t, "k")
	if a.ID == b.ID {
		t.Fatalf("expected distinct ids without idempotency keys")
	}
}

// ---------- Claim ----------

func TestClaim_FlipsToActiveAndCreatesRun(t *testing.T) {
	resetDB(t)
	job := mustEnqueue(t, "k")
	res, found, err := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-1", LeaseSec: 30})
	if err != nil || !found {
		t.Fatalf("claim found=%v err=%v", found, err)
	}
	if res.Job.ID != job.ID || res.Job.Status != model.StatusActive {
		t.Fatalf("job after claim: id=%s status=%s", res.Job.ID, res.Job.Status)
	}
	if res.Job.AttemptCount != 1 {
		t.Fatalf("attempt_count=%d, want 1", res.Job.AttemptCount)
	}
	if res.Run.JobID != job.ID || res.Run.Attempt != 1 || res.Run.Status != model.RunStatusRunning {
		t.Fatalf("run: job_id=%s attempt=%d status=%s", res.Run.JobID, res.Run.Attempt, res.Run.Status)
	}
}

func TestClaim_RespectsScheduledAtFuture(t *testing.T) {
	resetDB(t)
	_ = mustEnqueue(t, "k", withScheduledAt(time.Now().Add(1*time.Hour)))
	_, found, err := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w", LeaseSec: 30})
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("scheduled-future job should not be claimable")
	}
}

func TestClaim_FilterByKind(t *testing.T) {
	resetDB(t)
	_ = mustEnqueue(t, "a")
	_, found, _ := Claim(ClaimParams{Kinds: []string{"b"}, WorkerID: "w", LeaseSec: 30})
	if found {
		t.Fatal("claim with mismatched kind should not find a job")
	}
}

// TestClaim_AtMostOnceConcurrent races N goroutines on a single
// pending job. Exactly one should win — the FOR UPDATE SKIP LOCKED
// row lock guarantees that. The others should report found=false.
func TestClaim_AtMostOnceConcurrent(t *testing.T) {
	resetDB(t)
	_ = mustEnqueue(t, "race")

	const racers = 10
	var wins atomic.Int32
	var wg sync.WaitGroup
	wg.Add(racers)
	for i := 0; i < racers; i++ {
		go func() {
			defer wg.Done()
			_, found, err := Claim(ClaimParams{
				Kinds: []string{"race"}, WorkerID: "w", LeaseSec: 30,
			})
			if err != nil {
				t.Errorf("racer err: %v", err)
				return
			}
			if found {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("wins=%d, want exactly 1", got)
	}
}

// ---------- Heartbeat ----------

func TestHeartbeat_ExtendsLeaseAndProgress(t *testing.T) {
	resetDB(t)
	res := mustClaimNew(t, "k", "w-1")
	cur := int64(7)
	tot := int64(10)
	run, cancelRequested, err := Heartbeat(res.Run.ID, "w-1", ProgressUpdate{Current: &cur, Total: &tot}, 30)
	if err != nil {
		t.Fatal(err)
	}
	if cancelRequested {
		t.Fatalf("cancel_requested should default false on fresh job")
	}
	if run.ProgressCurrent != 7 || run.ProgressTotal != 10 {
		t.Fatalf("progress: %d/%d", run.ProgressCurrent, run.ProgressTotal)
	}
	if run.LeaseExpiresAt == nil || run.LeaseExpiresAt.Before(time.Now()) {
		t.Fatalf("lease not extended: %v", run.LeaseExpiresAt)
	}
}

func TestHeartbeat_WrongWorkerNotOwned(t *testing.T) {
	resetDB(t)
	res := mustClaimNew(t, "k", "w-1")
	_, _, err := Heartbeat(res.Run.ID, "different-worker", ProgressUpdate{}, 30)
	if !errors.Is(err, ErrNotOwned) {
		t.Fatalf("err=%v, want ErrNotOwned", err)
	}
}

// TestHeartbeat_SurfacesCancelRequested: workers observe cooperative
// cancel via the heartbeat response. After Cancel() flips the parent
// job's cancel_requested, the next Heartbeat returns true. This is the
// signal the Worker abstraction uses to cancel handler contexts.
func TestHeartbeat_SurfacesCancelRequested(t *testing.T) {
	resetDB(t)
	res := mustClaimNew(t, "k", "w-1")

	// Pre-cancel: false.
	_, cancelRequested, err := Heartbeat(res.Run.ID, "w-1", ProgressUpdate{}, 30)
	if err != nil || cancelRequested {
		t.Fatalf("pre-cancel: err=%v cancelRequested=%v", err, cancelRequested)
	}

	if _, err := Cancel(res.Job.ID); err != nil {
		t.Fatal(err)
	}

	// Post-cancel: true.
	_, cancelRequested, err = Heartbeat(res.Run.ID, "w-1", ProgressUpdate{}, 30)
	if err != nil {
		t.Fatal(err)
	}
	if !cancelRequested {
		t.Fatal("post-cancel: heartbeat should surface cancel_requested=true")
	}
}

// ---------- Complete ----------

func TestComplete_TerminalizesJobAndDenormsResult(t *testing.T) {
	resetDB(t)
	res := mustClaimNew(t, "k", "w-1")
	result := []byte(`{"ok":true,"count":42}`)
	job, err := Complete(res.Run.ID, "w-1", result)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != model.StatusSucceeded || job.CompletedAt == nil {
		t.Fatalf("job: status=%s completed_at=%v", job.Status, job.CompletedAt)
	}
	jsonEqual(t, job.Result, result)
	// Run side also marked succeeded.
	runs, _ := ListRuns(job.ID)
	if len(runs) != 1 || runs[0].Status != model.RunStatusSucceeded {
		t.Fatalf("runs: %+v", runs)
	}
}

// ---------- Fail ----------

func TestFail_RetryableBouncesBackToPending(t *testing.T) {
	resetDB(t)
	enqueued := mustEnqueue(t, "k", withMaxAttempts(3))
	res, _, _ := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-1", LeaseSec: 30})

	job, err := Fail(res.Run.ID, "w-1", "boom", true, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != model.StatusPending || job.CompletedAt != nil {
		t.Fatalf("retryable fail should leave job pending: status=%s completed=%v", job.Status, job.CompletedAt)
	}
	if job.AttemptCount != 1 {
		t.Fatalf("attempt_count=%d, want 1", job.AttemptCount)
	}
	// Original enqueue had no schedule_at; retry should set one.
	if enqueued.ScheduledAt != nil {
		t.Fatalf("test fixture: enqueued.ScheduledAt should be nil")
	}
}

func TestFail_TerminalWhenAttemptsExhausted(t *testing.T) {
	resetDB(t)
	_ = mustEnqueue(t, "k", withMaxAttempts(1))
	res, _, _ := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-1", LeaseSec: 30})

	job, err := Fail(res.Run.ID, "w-1", "boom", true /* retryable */, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	// max_attempts=1, one attempt used → terminal even though caller said retryable.
	if job.Status != model.StatusFailed || job.CompletedAt == nil {
		t.Fatalf("exhausted retries should terminalize: status=%s completed=%v", job.Status, job.CompletedAt)
	}
}

func TestFail_NonRetryableTerminalizesImmediately(t *testing.T) {
	resetDB(t)
	_ = mustEnqueue(t, "k", withMaxAttempts(5))
	res, _, _ := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-1", LeaseSec: 30})

	job, err := Fail(res.Run.ID, "w-1", "fatal", false, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != model.StatusFailed {
		t.Fatalf("non-retryable should terminalize: status=%s", job.Status)
	}
}

func TestFail_ResultLandsOnRunNotJob(t *testing.T) {
	resetDB(t)
	_ = mustEnqueue(t, "k", withMaxAttempts(2))
	res, _, _ := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-1", LeaseSec: 30})

	partial := []byte(`{"processed":42}`)
	job, err := Fail(res.Run.ID, "w-1", "halfway", true, 0, partial)
	if err != nil {
		t.Fatal(err)
	}
	if job.Result != nil {
		t.Fatalf("Job.result must stay nil on Fail; got %s", job.Result)
	}
	runs, _ := ListRuns(job.ID)
	if len(runs) != 1 {
		t.Fatalf("expected 1 run, got %d", len(runs))
	}
	jsonEqual(t, runs[0].Result, partial)
}

// ---------- Cancel ----------

func TestCancel_PendingFlipsToCancelled(t *testing.T) {
	resetDB(t)
	job := mustEnqueue(t, "k")
	out, err := Cancel(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != model.StatusCancelled || out.CompletedAt == nil {
		t.Fatalf("status=%s completed=%v", out.Status, out.CompletedAt)
	}
}

func TestCancel_ActiveSetsCancelRequested(t *testing.T) {
	resetDB(t)
	_ = mustEnqueue(t, "k")
	res, _, _ := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-1", LeaseSec: 30})
	out, err := Cancel(res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !out.CancelRequested {
		t.Fatal("cancel_requested should be true on active job")
	}
	if out.Status != model.StatusActive {
		t.Fatalf("status=%s (active stays active until worker observes the flag)", out.Status)
	}
}

// TestFail_OnCancelRequestedTerminalizesAsCancelled: when a worker
// fails its run on a job that has cancel_requested=true, the job
// terminalizes as 'cancelled' (not 'failed'), regardless of retries
// left or the retryable hint. Cancel intent wins.
func TestFail_OnCancelRequestedTerminalizesAsCancelled(t *testing.T) {
	resetDB(t)
	_ = mustEnqueue(t, "k", withMaxAttempts(5)) // plenty of retries left
	res, _, _ := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-1", LeaseSec: 30})

	// Request cancellation on the active job.
	if _, err := Cancel(res.Job.ID); err != nil {
		t.Fatal(err)
	}

	// Worker observed cancel_requested (via heartbeat in the real
	// flow) and Failed the run, even with retryable=true.
	out, err := Fail(res.Run.ID, "w-1", "context canceled", true, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != model.StatusCancelled {
		t.Fatalf("status=%s, want cancelled (cancel intent should beat retry)", out.Status)
	}
	if out.CompletedAt == nil {
		t.Fatal("cancelled job should have completed_at")
	}

	// The run itself is still 'failed' (it failed from the worker's
	// perspective) — only the parent job became 'cancelled'.
	runs, _ := ListRuns(res.Job.ID)
	if len(runs) != 1 || runs[0].Status != model.RunStatusFailed {
		t.Fatalf("expected 1 failed run, got %d runs status=%v", len(runs), runs)
	}
}

// ---------- LastRun / LastRunsForJobs ----------

func TestLastRun_NilWhenNeverClaimed(t *testing.T) {
	resetDB(t)
	job := mustEnqueue(t, "k")
	run, err := LastRun(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run != nil {
		t.Fatalf("expected nil for an unclaimed job, got attempt %d", run.Attempt)
	}
}

// The whole point of LastRun: CurrentRun goes nil the moment an attempt
// finishes, so a bounced job has nothing to show without this.
func TestLastRun_SurvivesFailedAttemptWhereCurrentRunDoesNot(t *testing.T) {
	resetDB(t)
	_ = mustEnqueue(t, "k", withMaxAttempts(3))
	res, _, _ := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-1", LeaseSec: 30})
	if _, err := Fail(res.Run.ID, "w-1", "boom", true, 0, []byte(`{"rows":7}`)); err != nil {
		t.Fatal(err)
	}

	current, err := CurrentRun(res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current != nil {
		t.Fatalf("CurrentRun should be nil after the attempt failed, got %s", current.Status)
	}

	last, err := LastRun(res.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if last == nil {
		t.Fatal("LastRun must return the failed attempt")
	}
	if last.Status != model.RunStatusFailed || last.Error != "boom" {
		t.Fatalf("expected the failed attempt, got status=%s error=%q", last.Status, last.Error)
	}
	jsonEqual(t, last.Result, []byte(`{"rows":7}`))
}

func TestLastRun_ReturnsHighestAttempt(t *testing.T) {
	resetDB(t)
	_ = mustEnqueue(t, "k", withMaxAttempts(3))
	first, _, _ := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-1", LeaseSec: 30})
	if _, err := Fail(first.Run.ID, "w-1", "first", true, 0, nil); err != nil {
		t.Fatal(err)
	}
	second, found, err := Claim(ClaimParams{Kinds: []string{"k"}, WorkerID: "w-2", LeaseSec: 30})
	if err != nil || !found {
		t.Fatalf("re-claim: found=%v err=%v", found, err)
	}

	last, err := LastRun(second.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if last == nil || last.Attempt != 2 {
		t.Fatalf("expected attempt 2, got %+v", last)
	}
	// Attempt 2 is in flight, so here last_run and current_run agree.
	if last.Status != model.RunStatusRunning {
		t.Fatalf("expected the running attempt, got %s", last.Status)
	}
}

func TestLastRunsForJobs_OneRowPerJobAtHighestAttempt(t *testing.T) {
	resetDB(t)
	// Job A: two attempts, newest failed. Job B: one running attempt.
	// Job C: never claimed, so it must be absent from the map.
	_ = mustEnqueue(t, "a", withMaxAttempts(3))
	a1, _, _ := Claim(ClaimParams{Kinds: []string{"a"}, WorkerID: "w-1", LeaseSec: 30})
	if _, err := Fail(a1.Run.ID, "w-1", "a-first", true, 0, nil); err != nil {
		t.Fatal(err)
	}
	a2, _, _ := Claim(ClaimParams{Kinds: []string{"a"}, WorkerID: "w-1", LeaseSec: 30})
	if _, err := Fail(a2.Run.ID, "w-1", "a-second", true, 0, nil); err != nil {
		t.Fatal(err)
	}
	b := mustClaimNew(t, "b", "w-2")
	c := mustEnqueue(t, "c")

	got, err := LastRunsForJobs([]string{a2.Job.ID, b.Job.ID, c.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 entries (c was never claimed), got %d: %+v", len(got), got)
	}
	ra, ok := got[a2.Job.ID]
	if !ok {
		t.Fatal("job a missing")
	}
	if ra.Attempt != 2 || ra.Error != "a-second" {
		t.Fatalf("job a: expected attempt 2 / a-second, got attempt %d / %q", ra.Attempt, ra.Error)
	}
	rb, ok := got[b.Job.ID]
	if !ok {
		t.Fatal("job b missing")
	}
	if rb.Status != model.RunStatusRunning {
		t.Fatalf("job b: expected running, got %s", rb.Status)
	}
	if _, ok := got[c.ID]; ok {
		t.Fatal("job c has no runs and must not appear in the map")
	}
}

func TestLastRunsForJobs_EmptyInputNoQuery(t *testing.T) {
	resetDB(t)
	got, err := LastRunsForJobs(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty map, got %+v", got)
	}
}

// ---------- helpers ----------

func mustClaimNew(t *testing.T, kind, worker string) ClaimResult {
	t.Helper()
	_ = mustEnqueue(t, kind)
	res, found, err := Claim(ClaimParams{Kinds: []string{kind}, WorkerID: worker, LeaseSec: 30})
	if err != nil || !found {
		t.Fatalf("setup claim: found=%v err=%v", found, err)
	}
	return res
}

func ptr[T any](v T) *T { return &v }

// keep gorm DSN test imports happy
var _ = database.DB
