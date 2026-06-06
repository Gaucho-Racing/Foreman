package service

import (
	"math/rand/v2"
	"time"

	"github.com/gaucho-racing/foreman/config"
	"github.com/gaucho-racing/foreman/pkg/logger"
)

// StartScheduler kicks off the recurring-job tick loop. Mirrors
// StartReaper: random initial jitter so simultaneously-booted replicas
// don't fire in lockstep, then a fixed-interval tick. Concurrent
// scheduler instances are safe — Tick uses FOR UPDATE SKIP LOCKED on
// each due schedule.
//
// The tick interval is short by default (1s) so jobs scheduled for a
// specific minute fire within a second of that minute. Cron precision
// is per-minute (5-field) or finer (@every 5s) — the scheduler is the
// gate on actual delivery latency.
func StartScheduler() {
	interval := time.Duration(config.SchedulerIntervalSec) * time.Second
	jitter := time.Duration(rand.Int64N(int64(interval) + 1))
	logger.SugarLogger.Infof("[SCHEDULER] starting (tick=%s, initial jitter=%s)", interval, jitter)
	go func() {
		time.Sleep(jitter)
		for {
			if n, err := Tick(); err != nil {
				logger.SugarLogger.Errorf("[SCHEDULER] tick failed: %v", err)
			} else if n > 0 {
				logger.SugarLogger.Infof("[SCHEDULER] fired %d schedule(s)", n)
			}
			time.Sleep(interval)
		}
	}()
}
