package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gaucho-racing/foreman/model"
	"github.com/gaucho-racing/foreman/service"
	"github.com/gin-gonic/gin"
)

// ---------- Enqueue ----------

type enqueueRequest struct {
	Kind           string          `json:"kind" binding:"required"`
	Queue          string          `json:"queue"`
	Service        string          `json:"service"`
	IdempotencyKey *string         `json:"idempotency_key"`
	Params         json.RawMessage `json:"params"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	ScheduledAt    *time.Time      `json:"scheduled_at"`
}

func EnqueueJob(c *gin.Context) {
	var req enqueueRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	job, err := service.Enqueue(service.EnqueueParams{
		Kind:           req.Kind,
		Queue:          req.Queue,
		Service:        req.Service,
		IdempotencyKey: req.IdempotencyKey,
		Params:         model.JSON(req.Params),
		Priority:       req.Priority,
		MaxAttempts:    req.MaxAttempts,
		ScheduledAt:    req.ScheduledAt,
	})
	if errors.Is(err, service.ErrConflict) {
		// Normalized error envelope: every non-2xx carries `error`. The
		// already-existing job is alongside for callers that want to skip
		// without an extra read.
		c.JSON(http.StatusConflict, gin.H{"error": err.Error(), "job": job})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, job)
}

// ---------- Claim (job-scoped, returns {job, run}) ----------

type claimRequest struct {
	Kinds    []string `json:"kinds" binding:"required"`
	Queues   []string `json:"queues"`
	WorkerID string   `json:"worker_id" binding:"required"`
	LeaseSec int      `json:"lease_seconds"`
}

func ClaimJob(c *gin.Context) {
	var req claimRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	res, found, err := service.Claim(service.ClaimParams{
		Kinds:    req.Kinds,
		Queues:   req.Queues,
		WorkerID: req.WorkerID,
		LeaseSec: req.LeaseSec,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !found {
		c.Status(http.StatusNoContent)
		return
	}
	// Workers need both: the job for kind/params, the run for the lease
	// + its id (used in every subsequent /runs/:id mutation).
	c.JSON(http.StatusOK, gin.H{"job": res.Job, "run": res.Run})
}

// ---------- Cancel (job-scoped) ----------

func CancelJob(c *gin.Context) {
	job, err := service.Cancel(c.Param("id"))
	if respondServiceErr(c, err) {
		return
	}
	c.JSON(http.StatusOK, job)
}

// ---------- Run lifecycle (run-scoped) ----------

type heartbeatRequest struct {
	WorkerID        string  `json:"worker_id" binding:"required"`
	ProgressCurrent *int64  `json:"progress_current"`
	ProgressTotal   *int64  `json:"progress_total"`
	ProgressMessage *string `json:"progress_message"`
	LeaseSec        int     `json:"lease_seconds"`
}

// heartbeatResponse marshals all JobRun fields at the top level plus a
// sibling `cancel_requested` lifted off the parent job — workers use it
// as the cooperative-cancel signal without an extra GetJob round-trip.
// cancel_requested is not part of the JobRun model (it's a Job column),
// so we splice it in here at the API boundary.
type heartbeatResponse struct {
	model.JobRun
	CancelRequested bool `json:"cancel_requested"`
}

func HeartbeatRun(c *gin.Context) {
	var req heartbeatRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	run, cancelRequested, err := service.Heartbeat(c.Param("id"), req.WorkerID, service.ProgressUpdate{
		Current: req.ProgressCurrent,
		Total:   req.ProgressTotal,
		Message: req.ProgressMessage,
	}, req.LeaseSec)
	if respondServiceErr(c, err) {
		return
	}
	c.JSON(http.StatusOK, heartbeatResponse{JobRun: run, CancelRequested: cancelRequested})
}

type completeRequest struct {
	WorkerID string          `json:"worker_id" binding:"required"`
	Result   json.RawMessage `json:"result"`
}

func CompleteRun(c *gin.Context) {
	var req completeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	job, err := service.Complete(c.Param("id"), req.WorkerID, model.JSON(req.Result))
	if respondServiceErr(c, err) {
		return
	}
	c.JSON(http.StatusOK, job)
}

type failRequest struct {
	WorkerID   string          `json:"worker_id" binding:"required"`
	Error      string          `json:"error"`
	Retryable  bool            `json:"retryable"`
	BackoffSec int             `json:"backoff_seconds"`
	// Result is optional — workers can attach partial data alongside a
	// failure. It lands on the JobRun only; Job.result remains reserved
	// for the winning attempt's payload.
	Result json.RawMessage `json:"result"`
}

func FailRun(c *gin.Context) {
	var req failRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	job, err := service.Fail(
		c.Param("id"),
		req.WorkerID,
		req.Error,
		req.Retryable,
		time.Duration(req.BackoffSec)*time.Second,
		model.JSON(req.Result),
	)
	if respondServiceErr(c, err) {
		return
	}
	c.JSON(http.StatusOK, job)
}

// ---------- Job reads ----------

// jobWithRun is the response shape when ?include= is set on /jobs or
// /jobs/:id. CurrentRun is null when no in-flight run exists. LastRun is
// omitted entirely unless asked for, so include=current_run responses stay
// byte-identical to what they were before last_run existed.
type jobWithRun struct {
	model.Job
	CurrentRun *model.JobRun `json:"current_run"`
	LastRun    *model.JobRun `json:"last_run,omitempty"`
}

// includeSet parses the comma-separated ?include= param. Two values are
// recognized:
//
//   - current_run — the in-flight attempt; null unless the job is active.
//   - last_run    — the newest attempt whatever its status, so pending and
//     terminal jobs still carry progress / error / result.
//
// Unknown values are ignored rather than rejected, so adding one later
// can't break a client that already sends it.
func includeSet(c *gin.Context) map[string]bool {
	out := map[string]bool{}
	for _, part := range strings.Split(c.Query("include"), ",") {
		if p := strings.TrimSpace(part); p != "" {
			out[p] = true
		}
	}
	return out
}

func GetJob(c *gin.Context) {
	job, err := service.Get(c.Param("id"))
	if respondServiceErr(c, err) {
		return
	}
	inc := includeSet(c)
	if !inc["current_run"] && !inc["last_run"] {
		c.JSON(http.StatusOK, job)
		return
	}
	out := jobWithRun{Job: job}
	if inc["current_run"] {
		run, err := service.CurrentRun(job.ID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		out.CurrentRun = run
	}
	if inc["last_run"] {
		run, err := service.LastRun(job.ID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		out.LastRun = run
	}
	c.JSON(http.StatusOK, out)
}

func ListJobs(c *gin.Context) {
	limit, _ := strconv.Atoi(c.Query("limit"))
	jobs, err := service.List(service.ListFilter{
		Status:  c.Query("status"),
		Kind:    c.Query("kind"),
		Service: c.Query("service"),
		Queue:   c.Query("queue"),
		Limit:   limit,
		Cursor:  c.Query("cursor"),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	inc := includeSet(c)
	if !inc["current_run"] && !inc["last_run"] {
		c.JSON(http.StatusOK, jobs)
		return
	}
	ids := make([]string, len(jobs))
	for i, j := range jobs {
		ids[i] = j.ID
	}
	var current, last map[string]model.JobRun
	if inc["current_run"] {
		current, err = service.CurrentRunsForJobs(ids)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}
	if inc["last_run"] {
		last, err = service.LastRunsForJobs(ids)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}
	out := make([]jobWithRun, len(jobs))
	for i, j := range jobs {
		out[i] = jobWithRun{
			Job:        j,
			CurrentRun: runFor(current, j.ID),
			LastRun:    runFor(last, j.ID),
		}
	}
	c.JSON(http.StatusOK, out)
}

// runFor pulls a job's run out of a batch map as a pointer. The local copy
// matters: taking &m[id] isn't allowed for maps, and reusing a loop variable's
// address would leave every row pointing at the last one.
func runFor(m map[string]model.JobRun, jobID string) *model.JobRun {
	r, ok := m[jobID]
	if !ok {
		return nil
	}
	return &r
}

// ListJobRuns returns every attempt at a job, oldest first. 404s match
// GetJob: a missing job id returns 404 (instead of an empty list) so
// callers can disambiguate "no runs yet" from "wrong id".
func ListJobRuns(c *gin.Context) {
	id := c.Param("id")
	if _, err := service.Get(id); err != nil {
		respondServiceErr(c, err)
		return
	}
	runs, err := service.ListRuns(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, runs)
}

// ---------- Run reads ----------

func GetRun(c *gin.Context) {
	run, err := service.GetRun(c.Param("id"))
	if respondServiceErr(c, err) {
		return
	}
	c.JSON(http.StatusOK, run)
}

func ListAllRuns(c *gin.Context) {
	limit, _ := strconv.Atoi(c.Query("limit"))
	runs, err := service.ListAllRuns(service.ListRunsFilter{
		Status:   c.Query("status"),
		WorkerID: c.Query("worker_id"),
		JobID:    c.Query("job_id"),
		Kind:     c.Query("kind"),
		Limit:    limit,
		Cursor:   c.Query("cursor"),
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, runs)
}

// ---------- helper ----------

// respondServiceErr maps service-layer sentinels to HTTP status codes
// and reports whether the request was already answered. All responses
// use the normalized {"error": "..."} shape.
func respondServiceErr(c *gin.Context, err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, service.ErrNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	case errors.Is(err, service.ErrNotOwned):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
	return true
}
