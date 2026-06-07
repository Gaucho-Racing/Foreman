package model

import "github.com/gaucho-racing/foreman/config"

// Canonical table identifiers. These return config.TablePrefix +
// suffix at call time so the prefix configured in Verify() is
// reflected by both GORM (via the TableName() methods that call
// these) and any hand-written Raw/Exec SQL in service/ or pkg/.
//
// Always go through these helpers — never hardcode "jobs" /
// "job_runs" / "schedules" in raw SQL. The prefix is controlled by
// FOREMAN_TABLE_PREFIX (default "foreman_") so a hardcoded name will
// silently target the wrong table when Foreman shares a database
// with another application.
//
// The strings concatenate without allocation in the prefix="" case
// (Go elides the concat) and allocate once per call otherwise — fine
// for query construction.

func TableJobs() string      { return config.TablePrefix + "jobs" }
func TableJobRuns() string   { return config.TablePrefix + "job_runs" }
func TableSchedules() string { return config.TablePrefix + "schedules" }
