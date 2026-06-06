package main

import (
	"github.com/gaucho-racing/foreman/api"
	"github.com/gaucho-racing/foreman/config"
	"github.com/gaucho-racing/foreman/database"
	"github.com/gaucho-racing/foreman/pkg/logger"
	"github.com/gaucho-racing/foreman/service"
)

func main() {
	logger.Init(config.IsProduction())
	defer logger.Logger.Sync()

	config.Verify()
	config.PrintStartupBanner()
	database.Init()
	service.StartReaper()
	service.StartScheduler()

	api.Run()
}
