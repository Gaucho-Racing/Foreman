package config

import (
	"fmt"
	"os"
	"strings"
)

type ServiceInfo struct {
	Name    string
	Version string
}

func (s ServiceInfo) FormattedNameWithVersion() string {
	return fmt.Sprintf("%s v%s", s.Name, s.Version)
}

func (s ServiceInfo) PathPrefix() string {
	return strings.ToLower(s.Name)
}

var Service = ServiceInfo{
	Name:    "Foreman",
	Version: "1.0.0",
}

// All endpoints are public for now — no auth layer. Re-add a
// RequireToken middleware in api/api.go (and a FOREMAN_TOKEN env)
// when locking down for shared deployments.

// ReaperIntervalSec is how often expired leases are swept back to pending.
var ReaperIntervalRaw = os.Getenv("FOREMAN_REAPER_INTERVAL_SEC")
var ReaperIntervalSec int

// DefaultLeaseSec is the lease length applied when a claim omits one.
var DefaultLeaseRaw = os.Getenv("FOREMAN_DEFAULT_LEASE_SEC")
var DefaultLeaseSec int

// SchedulerIntervalSec is how often the scheduler tick runs. Short by
// default (1s) so "fire every minute" actually fires within ~1s of the
// minute boundary.
var SchedulerIntervalRaw = os.Getenv("FOREMAN_SCHEDULER_INTERVAL_SEC")
var SchedulerIntervalSec int

// RetentionDays prunes terminal jobs (succeeded/failed/cancelled)
// older than this many days. Zero (the default) disables retention —
// rows accumulate forever. Cleanup happens inside the reaper loop;
// FK CASCADE means deleting a job auto-deletes its runs.
var RetentionDaysRaw = os.Getenv("FOREMAN_RETENTION_DAYS")
var RetentionDays int

var Env = os.Getenv("ENV")
var Port = os.Getenv("PORT")

var DatabaseHost = os.Getenv("DATABASE_HOST")
var DatabasePort = os.Getenv("DATABASE_PORT")
var DatabaseUser = os.Getenv("DATABASE_USER")
var DatabasePassword = os.Getenv("DATABASE_PASSWORD")
var DatabaseName = os.Getenv("DATABASE_NAME")

func IsProduction() bool {
	return Env == "PROD"
}
