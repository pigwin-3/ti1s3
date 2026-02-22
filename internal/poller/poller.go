package poller

import (
	"context"
	"log"
	"net/http"
	"time"

	"ti1s3/internal/config"
	"ti1s3/internal/entur"
	"ti1s3/internal/health"
	"ti1s3/internal/s3store"
)

func Run(cfg config.Config, httpClient *http.Client, state *health.State, storage *s3store.Client) {

	log.Printf("starting poller with requestorId=%s interval=%s retention_default=%s retention_used=%s", cfg.RequestorID, cfg.PollInterval, cfg.RetentionTTL, cfg.UsedRetentionTTL)
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	uploadOnce := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		xmlData, err := entur.FetchXML(ctx, httpClient, cfg.EnturBaseURL, cfg.RequestorID)
		if err != nil {
			state.MarkFailure(err.Error())
			log.Printf("fetch failed: %v", err)
			return
		}

		objectKey := time.Now().UTC().Format("20060102150405") + "-et.xml"
		uploadStartedAt := time.Now()
		if err := storage.UploadXML(ctx, objectKey, xmlData); err != nil {
			state.MarkFailure(err.Error())
			log.Printf("upload failed after %s: %v", time.Since(uploadStartedAt).Round(time.Millisecond), err)
			return
		}

		usedFiles, err := storage.UsedFilesSet(ctx)
		if err != nil {
			state.MarkFailure(err.Error())
			log.Printf("failed to load used file index: %v", err)
			return
		}

		cleanupStartedAt := time.Now()
		retentionCutoff := time.Now().UTC().Add(-cfg.RetentionTTL)
		usedRetentionCutoff := time.Now().UTC().Add(-cfg.UsedRetentionTTL)
		if err := storage.DeleteExpiredObjects(ctx, retentionCutoff, usedRetentionCutoff, usedFiles); err != nil {
			state.MarkFailure(err.Error())
			log.Printf("cleanup failed after %s: %v", time.Since(cleanupStartedAt).Round(time.Millisecond), err)
			return
		}

		state.MarkSuccess(objectKey)
		log.Printf("uploaded %s (%d bytes) in %s", objectKey, len(xmlData), time.Since(uploadStartedAt).Round(time.Millisecond))
	}

	uploadOnce()
	for range ticker.C {
		uploadOnce()
	}
}
