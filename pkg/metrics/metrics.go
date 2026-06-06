// Package metrics is Foreman's Prometheus surface. Three layers:
//
//  1. Counters / histograms incremented from the service layer at the
//     moment a state transition happens (enqueue, claim, complete,
//     fail, schedule fire, reaper sweep, retention prune).
//  2. A custom collector that runs aggregate SELECTs on the Postgres
//     tables every scrape — queue depth, oldest pending age, lease
//     pressure, schedule due-count. Cheaper than maintaining gauges
//     in code and always accurate vs. wall clock.
//  3. The standard Go / process collectors so /metrics also exposes
//     runtime stats (heap, goroutines, FDs).
//
// Everything is registered to a private registry exposed via
// Handler() so /foreman/metrics is its own namespace — no leakage
// from any third-party library that might MustRegister globally.
//
// Cardinality: counters and histograms include a `kind` label, which
// is user-defined. Deployments with hundreds of distinct kinds will
// see proportional time-series growth. Skip the labels in your own
// scraping config if that becomes a problem.
package metrics

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/gaucho-racing/foreman/database"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ---------- Counters ----------

var (
	JobsEnqueued = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "foreman_jobs_enqueued_total",
			Help: "Total enqueue calls. outcome=created on a fresh insert, deduped on an idempotency-key collision.",
		},
		[]string{"kind", "queue", "outcome"},
	)

	JobsClaimed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "foreman_jobs_claimed_total",
			Help: "Total successful claims (each one creates a new JobRun).",
		},
		[]string{"kind", "queue"},
	)

	JobsTerminal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "foreman_jobs_terminal_total",
			Help: "Total jobs that reached a terminal status (succeeded|failed|cancelled).",
		},
		[]string{"kind", "status"},
	)

	RunsTerminal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "foreman_runs_terminal_total",
			Help: "Total job runs that reached a terminal status (succeeded|failed|abandoned).",
		},
		[]string{"kind", "status"},
	)

	Heartbeats = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "foreman_heartbeats_total",
			Help: "Total heartbeat calls.",
		},
		[]string{"kind"},
	)

	SchedulesFired = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "foreman_schedules_fired_total",
		Help: "Total schedule fires (scheduled + manual /fire).",
	})

	ReaperSweeps = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "foreman_reaper_sweeps_total",
			Help: "Reaper tick outcomes. outcome=success|error.",
		},
		[]string{"outcome"},
	)

	ReaperReaped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "foreman_reaper_reaped_total",
		Help: "Total abandoned leases reclaimed by the reaper.",
	})

	RetentionDeleted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "foreman_retention_deleted_total",
		Help: "Total terminal jobs deleted by the retention sweep.",
	})
)

// ---------- Histograms ----------

// Wide-range buckets so we cover both <1s ack/heartbeat paths and
// hour-long batch jobs without a dozen empty buckets at the top end.
var durationBuckets = []float64{0.05, 0.25, 1, 5, 15, 60, 300, 1800, 7200}

var (
	JobLifetime = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "foreman_job_lifetime_seconds",
			Help:    "Time from enqueue to terminal status, by kind + status.",
			Buckets: durationBuckets,
		},
		[]string{"kind", "status"},
	)

	RunDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "foreman_run_duration_seconds",
			Help:    "Time from claim to terminal run status (succeeded|failed|abandoned).",
			Buckets: durationBuckets,
		},
		[]string{"kind", "status"},
	)

	ClaimWait = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "foreman_claim_wait_seconds",
			Help:    "Time from enqueue to first claim (queue wait).",
			Buckets: []float64{0.001, 0.01, 0.1, 1, 5, 30, 60, 300, 1800},
		},
		[]string{"kind"},
	)
)

// ---------- Registry + handler ----------

var registry = prometheus.NewRegistry()

func init() {
	registry.MustRegister(
		// counters
		JobsEnqueued, JobsClaimed, JobsTerminal, RunsTerminal, Heartbeats,
		SchedulesFired, ReaperSweeps, ReaperReaped, RetentionDeleted,
		// histograms
		JobLifetime, RunDuration, ClaimWait,
		// runtime
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)
}

// RegisterDBCollector hooks up the scrape-time SQL collector. Call
// after database.Init() so database.DB is non-nil.
func RegisterDBCollector() {
	registry.MustRegister(newDBCollector())
}

// Handler is the /foreman/metrics endpoint. Uses a private registry so
// nothing else accidentally surfaces here.
func Handler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		// Tight timeout — these queries should be <100ms even on a
		// loaded DB. If they're slower than 5s, the scrape should
		// fail loud rather than block forever.
		Timeout: 5 * time.Second,
	})
}

// ---------- DB-backed gauges (scraped fresh each time) ----------

type dbCollector struct {
	jobsByStatus       *prometheus.Desc
	runsByStatus       *prometheus.Desc
	schedulesByEnabled *prometheus.Desc
	oldestPendingAge   *prometheus.Desc
	overdueLeaseAge    *prometheus.Desc
	schedulesDue       *prometheus.Desc
}

func newDBCollector() *dbCollector {
	return &dbCollector{
		jobsByStatus: prometheus.NewDesc(
			"foreman_jobs_count",
			"Current count of jobs by status.",
			[]string{"status"}, nil,
		),
		runsByStatus: prometheus.NewDesc(
			"foreman_runs_count",
			"Current count of job runs by status.",
			[]string{"status"}, nil,
		),
		schedulesByEnabled: prometheus.NewDesc(
			"foreman_schedules_count",
			"Current count of schedules by enabled flag.",
			[]string{"enabled"}, nil,
		),
		oldestPendingAge: prometheus.NewDesc(
			"foreman_oldest_pending_age_seconds",
			"Age of the oldest pending job (now - enqueued_at). 0 when there are no pending jobs.",
			nil, nil,
		),
		overdueLeaseAge: prometheus.NewDesc(
			"foreman_overdue_lease_age_seconds",
			"Time the oldest overdue lease has been past its deadline. 0 when no leases are overdue (healthy).",
			nil, nil,
		),
		schedulesDue: prometheus.NewDesc(
			"foreman_schedules_due_count",
			"Schedules whose next_fire_at <= now() (scheduler backlog).",
			nil, nil,
		),
	}
}

func (c *dbCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.jobsByStatus
	ch <- c.runsByStatus
	ch <- c.schedulesByEnabled
	ch <- c.oldestPendingAge
	ch <- c.overdueLeaseAge
	ch <- c.schedulesDue
}

// Collect runs all queries in parallel? No — they're cheap enough that
// serial is fine and easier to reason about. Each query has a 2s
// per-statement timeout via context so a stuck Postgres doesn't lock
// up the scrape.
func (c *dbCollector) Collect(ch chan<- prometheus.Metric) {
	if database.DB == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	db := database.DB.WithContext(ctx)

	// jobs by status — emit every known status even when count=0 so
	// graphs don't get gaps when a category empties out.
	knownJobStatuses := []string{"pending", "active", "succeeded", "failed", "cancelled"}
	jobCounts := map[string]int64{}
	rows, err := db.Raw("SELECT status, count(*) FROM jobs GROUP BY status").Rows()
	if err == nil {
		jobCounts = scanCountMap(rows)
	}
	for _, s := range knownJobStatuses {
		ch <- prometheus.MustNewConstMetric(c.jobsByStatus, prometheus.GaugeValue, float64(jobCounts[s]), s)
	}

	knownRunStatuses := []string{"running", "succeeded", "failed", "abandoned"}
	runCounts := map[string]int64{}
	rows, err = db.Raw("SELECT status, count(*) FROM job_runs GROUP BY status").Rows()
	if err == nil {
		runCounts = scanCountMap(rows)
	}
	for _, s := range knownRunStatuses {
		ch <- prometheus.MustNewConstMetric(c.runsByStatus, prometheus.GaugeValue, float64(runCounts[s]), s)
	}

	scheduleCounts := map[string]int64{}
	rows, err = db.Raw("SELECT enabled::text, count(*) FROM schedules GROUP BY enabled").Rows()
	if err == nil {
		scheduleCounts = scanCountMap(rows)
	}
	for _, k := range []string{"true", "false"} {
		ch <- prometheus.MustNewConstMetric(c.schedulesByEnabled, prometheus.GaugeValue, float64(scheduleCounts[k]), k)
	}

	// Scalar gauges. NULL → 0 via COALESCE so we never emit NaN.
	var oldestPending sql.NullFloat64
	if err := db.Raw(`
		SELECT EXTRACT(EPOCH FROM (now() - min(enqueued_at)))
		FROM jobs WHERE status = 'pending'`).Scan(&oldestPending).Error; err == nil {
		ch <- prometheus.MustNewConstMetric(c.oldestPendingAge, prometheus.GaugeValue, nullFloat(oldestPending))
	}

	var overdueLease sql.NullFloat64
	if err := db.Raw(`
		SELECT EXTRACT(EPOCH FROM (now() - min(lease_expires_at)))
		FROM job_runs
		WHERE status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at < now()`).
		Scan(&overdueLease).Error; err == nil {
		ch <- prometheus.MustNewConstMetric(c.overdueLeaseAge, prometheus.GaugeValue, nullFloat(overdueLease))
	}

	var due int64
	if err := db.Raw("SELECT count(*) FROM schedules WHERE enabled AND next_fire_at <= now()").
		Scan(&due).Error; err == nil {
		ch <- prometheus.MustNewConstMetric(c.schedulesDue, prometheus.GaugeValue, float64(due))
	}
}

// scanCountMap turns a "SELECT key, count(*) ..." result into a map.
// Closes the rows iterator; defensive against partial scans.
func scanCountMap(rows *sql.Rows) map[string]int64 {
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var key string
		var count int64
		if err := rows.Scan(&key, &count); err != nil {
			return out
		}
		out[key] = count
	}
	return out
}

func nullFloat(n sql.NullFloat64) float64 {
	if !n.Valid {
		return 0
	}
	return n.Float64
}
