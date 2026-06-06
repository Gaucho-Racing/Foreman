package main

import (
	"github.com/gaucho-racing/foreman/api"
	"github.com/gaucho-racing/foreman/config"
	"github.com/gaucho-racing/foreman/database"
	"github.com/gaucho-racing/foreman/pkg/logger"
	"github.com/gaucho-racing/foreman/pkg/metrics"
	"github.com/gaucho-racing/foreman/service"
)

func main() {
	logger.Init(config.IsProduction())
	defer logger.Logger.Sync()

	config.Verify()
	config.PrintStartupBanner()
	database.Init()
	// Hook the scrape-time DB collector after database.Init so
	// database.DB is non-nil. The metric package's static counters /
	// histograms were already registered at process init.
	metrics.RegisterDBCollector()
	service.StartReaper()
	service.StartScheduler()

	api.Run()
}
