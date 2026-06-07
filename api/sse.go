package api

import (
	"time"

	"github.com/gaucho-racing/foreman/model"
	"github.com/gaucho-racing/foreman/service"
	"github.com/gin-gonic/gin"
)

// StreamJobEvents pushes the job's current state to the client as SSE,
// re-sending on a tick until the job reaches a terminal status or the
// client disconnects. Served at /foreman/events/:id (trailing-wildcard
// path) and configured envelope:passthrough in kerbecs so the stream is
// not wrapped.
//
// Each event carries the Job plus the in-flight Run (worker_id,
// progress_*, lease_expires_at, etc. — fields that moved to JobRun in
// the v2 schema). Subscribers that decode just the Job shape still
// work; the `current_run` field is null on terminal jobs.
func StreamJobEvents(c *gin.Context) {
	id := c.Param("id")
	job, err := service.Get(id)
	if respondServiceErr(c, err) {
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	c.SSEvent("job", buildJobEvent(job))
	c.Writer.Flush()
	if job.IsTerminal() {
		return
	}

	ctx := c.Request.Context()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			job, err := service.Get(id)
			if err != nil {
				return
			}
			c.SSEvent("job", buildJobEvent(job))
			c.Writer.Flush()
			if job.IsTerminal() {
				return
			}
		}
	}
}

// buildJobEvent wraps a Job with its in-flight Run for SSE consumers.
// Reuses the jobWithRun shape that /jobs?include=current_run already
// returns so the dashboard's decode path is identical for both. Errors
// looking up the run are swallowed — better to push the bare job than
// drop the event entirely.
func buildJobEvent(job model.Job) jobWithRun {
	run, _ := service.CurrentRun(job.ID)
	return jobWithRun{Job: job, CurrentRun: run}
}
