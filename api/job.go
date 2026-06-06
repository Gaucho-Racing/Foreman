package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
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

func HeartbeatRun(c *gin.Context) {
	var req heartbeatRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	run, err := service.Heartbeat(c.Param("id"), req.WorkerID, service.ProgressUpdate{
		Current: req.ProgressCurrent,
		Total:   req.ProgressTotal,
		Message: req.ProgressMessage,
	}, req.LeaseSec)
	if respondServiceErr(c, err) {
		return
	}
	c.JSON(http.StatusOK, run)
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

// jobWithRun is the response shape when ?include=current_run is set on
// /jobs or /jobs/:id. CurrentRun is null when no in-flight run exists.
type jobWithRun struct {
	model.Job
	CurrentRun *model.JobRun `json:"current_run"`
}

func GetJob(c *gin.Context) {
	job, err := service.Get(c.Param("id"))
	if respondServiceErr(c, err) {
		return
	}
	if c.Query("include") != "current_run" {
		c.JSON(http.StatusOK, job)
		return
	}
	run, err := service.CurrentRun(job.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, jobWithRun{Job: job, CurrentRun: run})
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
	if c.Query("include") != "current_run" {
		c.JSON(http.StatusOK, jobs)
		return
	}
	ids := make([]string, len(jobs))
	for i, j := range jobs {
		ids[i] = j.ID
	}
	runs, err := service.CurrentRunsForJobs(ids)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	out := make([]jobWithRun, len(jobs))
	for i, j := range jobs {
		var run *model.JobRun
		if r, ok := runs[j.ID]; ok {
			r := r // local copy so the pointer is stable across loop iterations
			run = &r
		}
		out[i] = jobWithRun{Job: j, CurrentRun: run}
	}
	c.JSON(http.StatusOK, out)
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
