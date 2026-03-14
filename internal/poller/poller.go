package poller

import (
	"context"
	"fmt"
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

		handleFailure := func(prefix string, err error) {
			state.MarkFailure(err.Error())
			log.Printf("%s: %v", prefix, err)

			logCtx, logCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer logCancel()

			if logErr := storage.AppendErrorLog(logCtx, fmt.Sprintf("%s: %v", prefix, err), time.Now().UTC()); logErr != nil {
				log.Printf("failed to write error log: %v", logErr)
			}
		}

		xmlData, err := entur.FetchXML(ctx, httpClient, cfg.EnturBaseURL, cfg.RequestorID)
		if err != nil {
			handleFailure("fetch failed", err)
			return
		}
		log.Printf("received payload from entur bytes=%d", len(xmlData))

		objectKey := time.Now().UTC().Format("20060102150405") + "-et.xml"
		uploadStartedAt := time.Now()
		if err := storage.UploadXML(ctx, objectKey, xmlData); err != nil {
			handleFailure(fmt.Sprintf("upload failed after %s", time.Since(uploadStartedAt).Round(time.Millisecond)), err)
			return
		}

		usedFiles, err := storage.UsedFilesSet(ctx)
		if err != nil {
			handleFailure("failed to load used file index", err)
			return
		}

		cleanupStartedAt := time.Now()
		retentionCutoff := time.Now().UTC().Add(-cfg.RetentionTTL)
		usedRetentionCutoff := time.Now().UTC().Add(-cfg.UsedRetentionTTL)
		if err := storage.DeleteExpiredObjects(ctx, retentionCutoff, usedRetentionCutoff, usedFiles); err != nil {
			handleFailure(fmt.Sprintf("cleanup failed after %s", time.Since(cleanupStartedAt).Round(time.Millisecond)), err)
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
