// Package client is the Go SDK for talking to a Foreman server. It covers
// both the producer side (Enqueue) and the worker side (Claim, Heartbeat,
// Complete, Fail) — a single service can play both roles.
//
// Construct a Client with New(endpoint), then call its methods. Clients
// are safe for concurrent use; the underlying http.Client is shared.
//
// Endpoint should be the base URL of the Foreman server (e.g.
// "http://foreman:7011"). All methods append the route path themselves.
// Passing an empty endpoint turns every call into a no-op success — handy
// for opt-in deployments where Foreman is configured per-environment.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Job is the subset of Foreman's Job record a worker needs to act on a
// claim. Other fields (priority, lease, worker_id, etc.) are tracked
// server-side and don't need to round-trip.
type Job struct {
	ID          string          `json:"id"`
	Kind        string          `json:"kind"`
	Queue       string          `json:"queue"`
	Service     string          `json:"service"`
	Params      json.RawMessage `json:"params"`
	Attempt     int             `json:"attempt"`
	MaxAttempts int             `json:"max_attempts"`
}

type EnqueueRequest struct {
	Kind           string          `json:"kind"`
	Queue          string          `json:"queue,omitempty"`
	Service        string          `json:"service,omitempty"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	Params         json.RawMessage `json:"params,omitempty"`
	Priority       int             `json:"priority,omitempty"`
	MaxAttempts    int             `json:"max_attempts,omitempty"`
	ScheduledAt    *time.Time      `json:"scheduled_at,omitempty"`
}

type ClaimRequest struct {
	Kinds    []string `json:"kinds"`
	Queues   []string `json:"queues,omitempty"`
	WorkerID string   `json:"worker_id"`
	LeaseSec int      `json:"lease_seconds,omitempty"`
}

type HeartbeatRequest struct {
	WorkerID        string  `json:"worker_id"`
	ProgressCurrent *int64  `json:"progress_current,omitempty"`
	ProgressTotal   *int64  `json:"progress_total,omitempty"`
	ProgressMessage *string `json:"progress_message,omitempty"`
	LeaseSec        int     `json:"lease_seconds,omitempty"`
}

type CompleteRequest struct {
	WorkerID string          `json:"worker_id"`
	Result   json.RawMessage `json:"result,omitempty"`
}

type FailRequest struct {
	WorkerID   string `json:"worker_id"`
	Error      string `json:"error,omitempty"`
	Retryable  bool   `json:"retryable"`
	BackoffSec int    `json:"backoff_seconds,omitempty"`
}

// EnqueueResult lets callers distinguish a fresh insert (201) from an
// idempotency-key collision (409). JobID is populated in both cases so
// fan-out callers can record what they queued.
type EnqueueResult struct {
	Created bool
	JobID   string
}

type Client struct {
	// Endpoint is the base URL of the Foreman server. An empty Endpoint
	// makes every method a no-op success; see package doc.
	Endpoint string
	// HTTPClient is the http.Client used for every call. Defaults to a
	// 30s-timeout client in New(); override to plug in custom transports.
	HTTPClient *http.Client
}

// New returns a Client targeting the given endpoint with a 30-second
// HTTP timeout. The endpoint may be empty to disable the client.
func New(endpoint string) *Client {
	return &Client{
		Endpoint:   endpoint,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// disabled reports whether the client is configured to no-op.
func (c *Client) disabled() bool { return c == nil || c.Endpoint == "" }

// Enqueue posts a new job. A 409 collision is folded into a non-error
// result with Created=false so idempotent retransmits don't bubble.
func (c *Client) Enqueue(ctx context.Context, req EnqueueRequest) (EnqueueResult, error) {
	if c.disabled() {
		return EnqueueResult{}, nil
	}
	resp, err := c.do(ctx, http.MethodPost, "/foreman/jobs", req)
	if err != nil {
		return EnqueueResult{}, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusCreated:
		var job Job
		if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
			return EnqueueResult{}, fmt.Errorf("decode created: %w", err)
		}
		return EnqueueResult{Created: true, JobID: job.ID}, nil
	case http.StatusConflict:
		var body struct {
			Job Job `json:"job"`
		}
		// Best-effort decode; the conflict status is the signal.
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return EnqueueResult{Created: false, JobID: body.Job.ID}, nil
	default:
		return EnqueueResult{}, statusErr("enqueue", resp)
	}
}

// Claim asks Foreman for one job matching any of req.Kinds. Returns
// (nil, nil) when nothing is available (server 204) — the expected
// "queue empty" signal, not an error.
func (c *Client) Claim(ctx context.Context, req ClaimRequest) (*Job, error) {
	if c.disabled() {
		return nil, nil
	}
	resp, err := c.do(ctx, http.MethodPost, "/foreman/claim", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var job Job
		if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
			return nil, fmt.Errorf("claim decode: %w", err)
		}
		return &job, nil
	case http.StatusNoContent:
		return nil, nil
	default:
		return nil, statusErr("claim", resp)
	}
}

// Heartbeat extends the lease on an in-flight job and reports optional
// progress. Workers must call this faster than their lease expires or
// the reaper will reclaim the job.
func (c *Client) Heartbeat(ctx context.Context, jobID string, req HeartbeatRequest) error {
	if c.disabled() {
		return nil
	}
	return c.simplePost(ctx, "/foreman/jobs/"+jobID+"/heartbeat", req, "heartbeat")
}

// Complete marks a job succeeded. The optional Result is stored verbatim
// as JSONB on the job row.
func (c *Client) Complete(ctx context.Context, jobID string, req CompleteRequest) error {
	if c.disabled() {
		return nil
	}
	return c.simplePost(ctx, "/foreman/jobs/"+jobID+"/complete", req, "complete")
}

// Fail records a failed attempt. With Retryable=true and attempts
// remaining the job returns to pending after BackoffSec; otherwise it's
// marked failed terminally.
func (c *Client) Fail(ctx context.Context, jobID string, req FailRequest) error {
	if c.disabled() {
		return nil
	}
	return c.simplePost(ctx, "/foreman/jobs/"+jobID+"/fail", req, "fail")
}

// Cancel cancels a pending job immediately, or flags a running job for
// cooperative cancellation. Terminal jobs return unchanged.
func (c *Client) Cancel(ctx context.Context, jobID string) error {
	if c.disabled() {
		return nil
	}
	// Cancel has no body — pass a struct{} to keep the same path.
	return c.simplePost(ctx, "/foreman/jobs/"+jobID+"/cancel", struct{}{}, "cancel")
}

func (c *Client) simplePost(ctx context.Context, path string, body any, op string) error {
	resp, err := c.do(ctx, http.MethodPost, path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return statusErr(op, resp)
	}
	return nil
}

func (c *Client) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var rdr io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal: %w", err)
		}
		rdr = bytes.NewReader(buf)
	}
	url := strings.TrimRight(c.Endpoint, "/") + path
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	hc := c.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}
	return hc.Do(req)
}

// statusErr drains and includes a small slice of the response body so
// callers get a useful error string instead of "responded 500".
func statusErr(op string, resp *http.Response) error {
	buf, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	msg := strings.TrimSpace(string(buf))
	if msg == "" {
		return fmt.Errorf("%s: foreman responded %d", op, resp.StatusCode)
	}
	return fmt.Errorf("%s: foreman responded %d: %s", op, resp.StatusCode, msg)
}

// ErrEmpty is exported for callers that want a sentinel for the
// "endpoint not configured, no-op" case. None of the Client methods
// return it today (we return nil instead) but it's reserved for future
// strict modes.
var ErrEmpty = errors.New("foreman: endpoint not configured")
