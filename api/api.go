package api

import (
	"fmt"
	"time"

	"github.com/gaucho-racing/foreman/config"
	"github.com/gaucho-racing/foreman/pkg/logger"
	"github.com/gaucho-racing/foreman/pkg/metrics"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func Run() {
	api := InitializeRouter()
	InitializeRoutes(api)
	err := api.Run(":" + config.Port)
	if err != nil {
		logger.SugarLogger.Fatalf("Failed to start server: %v", err)
	}
}

func InitializeRouter() *gin.Engine {
	if config.IsProduction() {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.Default()
	r.Use(cors.New(cors.Config{
		AllowAllOrigins:  true,
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Length", "Content-Type", "Authorization"},
		MaxAge:           12 * time.Hour,
		AllowCredentials: true,
	}))
	return r
}

func InitializeRoutes(router *gin.Engine) {
	p := config.Service.PathPrefix()
	router.GET(fmt.Sprintf("/%s/ping", p), Ping)
	// Standard Prometheus scrape target. Private registry — no leakage
	// from third-party libs that may MustRegister globally.
	router.GET(fmt.Sprintf("/%s/metrics", p), gin.WrapH(metrics.Handler()))

	// All endpoints are public for now — no auth layer. Re-add a
	// RequireToken middleware and a Group + .Use(...) for the writes
	// when locking down for shared deployments.

	// Job-scoped resources
	router.POST(fmt.Sprintf("/%s/jobs", p), EnqueueJob)
	router.GET(fmt.Sprintf("/%s/jobs", p), ListJobs)
	// Static "claim" before the :id wildcard so gin's router prefers it
	// for the literal path. Gin's tree handles this correctly when both
	// are registered, but order doesn't hurt.
	router.POST(fmt.Sprintf("/%s/jobs/claim", p), ClaimJob)
	router.GET(fmt.Sprintf("/%s/jobs/:id", p), GetJob)
	router.GET(fmt.Sprintf("/%s/jobs/:id/runs", p), ListJobRuns)
	router.GET(fmt.Sprintf("/%s/jobs/:id/events", p), StreamJobEvents)
	router.POST(fmt.Sprintf("/%s/jobs/:id/cancel", p), CancelJob)

	// Run-scoped resources. Workers act on the run they own — the run
	// id is what came back from /jobs/claim — not on the parent job.
	router.GET(fmt.Sprintf("/%s/runs", p), ListAllRuns)
	router.GET(fmt.Sprintf("/%s/runs/:id", p), GetRun)
	router.POST(fmt.Sprintf("/%s/runs/:id/heartbeat", p), HeartbeatRun)
	router.POST(fmt.Sprintf("/%s/runs/:id/complete", p), CompleteRun)
	router.POST(fmt.Sprintf("/%s/runs/:id/fail", p), FailRun)

	// Schedules — recurring (or future one-shot) recipes for enqueuing
	// jobs. A separate scheduler goroutine ticks and fires these.
	router.POST(fmt.Sprintf("/%s/schedules", p), CreateSchedule)
	router.GET(fmt.Sprintf("/%s/schedules", p), ListSchedules)
	router.GET(fmt.Sprintf("/%s/schedules/:id", p), GetSchedule)
	router.PUT(fmt.Sprintf("/%s/schedules/:id", p), UpdateSchedule)
	router.DELETE(fmt.Sprintf("/%s/schedules/:id", p), DeleteSchedule)
	router.POST(fmt.Sprintf("/%s/schedules/:id/fire", p), FireSchedule)
}
