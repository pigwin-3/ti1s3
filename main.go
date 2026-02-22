package main

import (
	"log"
	"net/http"
	"time"

	"ti1s3/internal/config"
	"ti1s3/internal/health"
	"ti1s3/internal/poller"
)

func main() {
	_ = config.LoadDotEnvFile(".env")

	startupTimestamp := time.Now().Format("20060102T150405")
	cfg, err := config.Load(startupTimestamp)
	if err != nil {
		log.Fatal(err)
	}

	httpClient := &http.Client{Timeout: 180 * time.Second}
	state := health.NewState()
	health.StartServer(cfg.HealthAddr, cfg.RequestorID, state)

	poller.Run(cfg, httpClient, state)
}
