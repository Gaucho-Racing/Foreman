package model

import "time"

// Schedule is a recurring (or one-shot) recipe for enqueuing Jobs. The
// scheduler goroutine ticks periodically, finds schedules whose
// NextFireAt has elapsed, enqueues a fresh Job using the schedule's
// kind/params/queue/etc., then advances NextFireAt via the cron
// expression. A Schedule is the *recipe*; the resulting Jobs are
// instances — same separation as Job/JobRun, one level up.
//
// CronExpr accepts standard 5-field cron ("0 9 * * *"), descriptors
// ("@hourly", "@daily"), and durations ("@every 5m"). Parsed by
// github.com/robfig/cron/v3 with the standard parser.
//
// Idempotency: each fire derives a deterministic idempotency key
// "sched:<schedule_id>:<fire_at-rfc3339>" so a scheduler retry after a
// crash (mid-fire) bumps into the existing Job rather than enqueuing a
// duplicate.
type Schedule struct {
	ID string `json:"id" gorm:"primaryKey"`

	// Job recipe — applied verbatim on every fire.
	Kind        string `json:"kind" gorm:"index;not null"`
	Queue       string `json:"queue" gorm:"not null;default:default"`
	Service     string `json:"service"`
	Params      JSON   `json:"params,omitempty" gorm:"type:jsonb"`
	Priority    int    `json:"priority" gorm:"not null;default:0"`
	MaxAttempts int    `json:"max_attempts" gorm:"not null;default:1"`

	// CronExpr drives NextFireAt. Validated at create/update time.
	CronExpr string `json:"cron_expr" gorm:"not null"`
	// Timezone for cron evaluation. Defaults to UTC. Names from the
	// IANA tz database ("America/Los_Angeles", "UTC").
	Timezone string `json:"timezone" gorm:"not null;default:UTC"`

	Enabled bool `json:"enabled" gorm:"not null;default:true;index"`

	// Operational state — updated by the scheduler on each fire.
	NextFireAt time.Time  `json:"next_fire_at" gorm:"index;not null"`
	LastFireAt *time.Time `json:"last_fire_at,omitempty"`
	LastJobID  string     `json:"last_job_id,omitempty"`

	CreatedAt time.Time `json:"created_at" gorm:"autoCreateTime;precision:6"`
	UpdatedAt time.Time `json:"updated_at" gorm:"autoUpdateTime;precision:6"`
}

func (Schedule) TableName() string { return "schedules" }
