package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"ti1s3/internal/config"
	"ti1s3/internal/entur"
	"ti1s3/internal/health"
	"ti1s3/internal/poller"
	"ti1s3/internal/s3store"
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
	storage := s3store.NewClient(httpClient, cfg)
	health.StartServer(cfg.HealthAddr, cfg.RequestorID, cfg.SubscribeCallbackPath, cfg.APIKeys, state, storage)
	log.Printf("startup complete requestorId=%s mode=%s health_addr=%s", cfg.RequestorID, cfg.Mode, cfg.HealthAddr)

	if cfg.SubscribeEnabled {
		if cfg.SubscribeConsumerAddress == "" {
			log.Fatalf("ENTUR_SUBSCRIBE_CONSUMER_ADDRESS is required when subscribe mode is enabled")
		}

		log.Printf("subscribe mode enabled subscribe_url=%s callback_path=%s consumer_address=%s heartbeat=%s initial_termination=%s auto_renew=%t renew_before=%s",
			cfg.EnturSubscribeURL,
			cfg.SubscribeCallbackPath,
			cfg.SubscribeConsumerAddress,
			cfg.SubscribeHeartbeatInterval,
			cfg.SubscribeInitialTermination,
			cfg.SubscribeAutoRenew,
			cfg.SubscribeRenewBeforeTermination,
		)

		subscribeOnce := func() (time.Time, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			result, err := entur.SubscribeET(ctx, httpClient, cfg)
			if err != nil {
				log.Printf("entur subscribe failed: %v", err)
				if result.Body != "" {
					log.Printf("entur subscribe response body: %s", result.Body)
				}
				return time.Time{}, err
			}

			log.Printf("entur subscribe succeeded status=%d renew_at=%s", result.StatusCode, result.NextRenewAt.Format(time.RFC3339))
			if result.Body != "" {
				log.Printf("entur subscribe response body: %s", result.Body)
			}

			return result.NextRenewAt, nil
		}

		nextRenewAt, err := subscribeOnce()
		if err != nil {
			log.Printf("continuing in subscribe mode; direct-delivery callback is still available")
			nextRenewAt = time.Now().Add(30 * time.Second)
		}

		if cfg.SubscribeAutoRenew {
			go func() {
				for {
					sleepFor := time.Until(nextRenewAt)
					if sleepFor < time.Second {
						sleepFor = time.Second
					}
					log.Printf("next subscribe renewal in %s", sleepFor.Round(time.Second))

					time.Sleep(sleepFor)
					updatedRenewAt, renewErr := subscribeOnce()
					if renewErr != nil {
						log.Printf("subscribe renewal failed, retrying in 30s")
						nextRenewAt = time.Now().Add(30 * time.Second)
						continue
					}

					nextRenewAt = updatedRenewAt
				}
			}()
		}

		log.Printf("subscribe mode running; waiting for Entur deliveries")
		select {}
	}

	log.Printf("poll mode enabled; starting periodic Entur fetches")
	poller.Run(cfg, httpClient, state, storage)
}
