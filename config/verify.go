package config

import (
	"os"
	"strconv"

	"github.com/gaucho-racing/foreman/pkg/logger"
)

func Verify() {
	if Env == "" {
		Env = "PROD"
		logger.SugarLogger.Infof("ENV is not set, defaulting to %s", Env)
	}
	if Port == "" {
		Port = "7011"
		logger.SugarLogger.Infof("PORT is not set, defaulting to %s", Port)
	}
	if DatabaseHost == "" {
		DatabaseHost = "localhost"
		logger.SugarLogger.Infof("DATABASE_HOST is not set, defaulting to %s", DatabaseHost)
	}
	if DatabasePort == "" {
		DatabasePort = "5432"
		logger.SugarLogger.Infof("DATABASE_PORT is not set, defaulting to %s", DatabasePort)
	}
	if DatabaseUser == "" {
		DatabaseUser = "postgres"
		logger.SugarLogger.Infof("DATABASE_USER is not set, defaulting to %s", DatabaseUser)
	}
	if DatabasePassword == "" {
		DatabasePassword = "password"
		logger.SugarLogger.Infof("DATABASE_PASSWORD is not set, defaulting to %s", DatabasePassword)
	}
	if DatabaseName == "" {
		DatabaseName = "foreman"
		logger.SugarLogger.Infof("DATABASE_NAME is not set, defaulting to %s", DatabaseName)
	}

	ReaperIntervalSec = intEnv(ReaperIntervalRaw, "FOREMAN_REAPER_INTERVAL_SEC", 10)
	DefaultLeaseSec = intEnv(DefaultLeaseRaw, "FOREMAN_DEFAULT_LEASE_SEC", 60)
	SchedulerIntervalSec = intEnv(SchedulerIntervalRaw, "FOREMAN_SCHEDULER_INTERVAL_SEC", 1)
	// 0 disables — accept that explicitly so the default behavior is
	// "keep forever," matching what existed before this knob.
	RetentionDays = intEnv(RetentionDaysRaw, "FOREMAN_RETENTION_DAYS", 0)

	// LookupEnv distinguishes "unset" (apply default) from "set to ''"
	// (caller explicitly wants no prefix). Either is valid; we just
	// can't conflate them with the usual ==""-means-default trick.
	if raw, ok := os.LookupEnv("FOREMAN_TABLE_PREFIX"); ok {
		TablePrefix = raw
	} else {
		TablePrefix = "foreman_"
		logger.SugarLogger.Infof("FOREMAN_TABLE_PREFIX is not set, defaulting to %q", TablePrefix)
	}
}

func intEnv(raw, name string, def int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		logger.SugarLogger.Errorf("%s=%q is not an int, defaulting to %d", name, raw, def)
		return def
	}
	return n
}
