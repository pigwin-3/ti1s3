package health

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"ti1s3/internal/s3store"
)

type State struct {
	ok                atomic.Bool
	lastSuccessUnix   atomic.Int64
	lastSuccessObject atomic.Value
	lastError         atomic.Value
}

func NewState() *State {
	return &State{}
}

func (state *State) MarkFailure(errText string) {
	state.ok.Store(false)
	if strings.TrimSpace(errText) == "" {
		errText = "unknown error"
	}
	state.lastError.Store(errText)
}

func (state *State) MarkSuccess(objectKey string) {
	state.ok.Store(true)
	state.lastSuccessUnix.Store(time.Now().Unix())
	state.lastSuccessObject.Store(objectKey)
	state.lastError.Store("")
}

func (state *State) Snapshot() (bool, int64, string, string, string) {
	ok := state.ok.Load()
	lastSuccessUnix := state.lastSuccessUnix.Load()
	lastSuccessRFC3339 := ""
	if lastSuccessUnix > 0 {
		lastSuccessRFC3339 = time.Unix(lastSuccessUnix, 0).UTC().Format(time.RFC3339)
	}

	lastObject := ""
	if value := state.lastSuccessObject.Load(); value != nil {
		if asString, castOK := value.(string); castOK {
			lastObject = asString
		}
	}

	lastError := ""
	if value := state.lastError.Load(); value != nil {
		if asString, castOK := value.(string); castOK {
			lastError = asString
		}
	}

	return ok, lastSuccessUnix, lastObject, lastSuccessRFC3339, lastError
}

func StartServer(addr string, requestorID string, callbackPath string, apiKeys []string, state *State, storage *s3store.Client) {
	mux := http.NewServeMux()
	requireAPIKey := apiKeyMiddleware(apiKeys)

	if strings.TrimSpace(callbackPath) == "" {
		callbackPath = "/entur/subscription"
	}
	if !strings.HasPrefix(callbackPath, "/") {
		callbackPath = "/" + callbackPath
	}

	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		ok, lastSuccessUnix, lastObject, lastSuccessRFC3339, lastError := state.Snapshot()
		statusText := "not ok"
		statusCode := http.StatusNotFound
		if ok {
			statusText = "ok"
			statusCode = http.StatusOK
		}

		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(statusCode)
		_ = json.NewEncoder(writer).Encode(map[string]interface{}{
			"status":             statusText,
			"serviceHealthy":     ok,
			"requestorId":        requestorID,
			"lastSuccessUnix":    lastSuccessUnix,
			"lastSuccessRFC3339": lastSuccessRFC3339,
			"lastSuccessObject":  lastObject,
			"lastError":          lastError,
			"apiProtected":       len(apiKeys) > 0,
		})
	})

	mux.HandleFunc("/health-status", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if state.ok.Load() {
			writer.WriteHeader(http.StatusOK)
			_, _ = writer.Write([]byte("ok"))
			return
		}

		writer.WriteHeader(http.StatusNotFound)
		_, _ = writer.Write([]byte("not ok"))
	})

	mux.Handle("/used-files", requireAPIKey(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		ctx, cancel := context.WithTimeout(request.Context(), 30*time.Second)
		defer cancel()

		usedFiles, err := storage.ListUsedFiles(ctx)
		if err != nil {
			writer.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(writer).Encode(map[string]string{"error": err.Error()})
			return
		}

		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]interface{}{
			"count": usedFilesCount(usedFiles),
			"files": usedFiles,
		})
	})))

	mux.Handle("/used-files/mark", requireAPIKey(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var payload struct {
			Key    string `json:"key"`
			UsedAt string `json:"usedAt"`
		}

		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(writer).Encode(map[string]string{"error": "invalid JSON body"})
			return
		}

		key := strings.TrimSpace(payload.Key)
		if key == "" {
			writer.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(writer).Encode(map[string]string{"error": "key is required"})
			return
		}

		usedAt := time.Now().UTC()
		if strings.TrimSpace(payload.UsedAt) != "" {
			parsed, err := time.Parse(time.RFC3339, payload.UsedAt)
			if err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(writer).Encode(map[string]string{"error": "usedAt must be RFC3339"})
				return
			}
			usedAt = parsed.UTC()
		}

		ctx, cancel := context.WithTimeout(request.Context(), 30*time.Second)
		defer cancel()

		if err := storage.MarkFileUsed(ctx, key, usedAt); err != nil {
			writer.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(writer).Encode(map[string]string{"error": err.Error()})
			return
		}

		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]string{"status": "ok"})
	})))

	mux.HandleFunc(callbackPath, func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// Store raw payload from Entur direct-delivery as a time-keyed XML snapshot.
		payload, err := io.ReadAll(io.LimitReader(request.Body, 20<<20))
		if err != nil {
			writer.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(writer).Encode(map[string]string{"error": "failed to read request body"})
			return
		}

		if len(payload) == 0 {
			writer.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(writer).Encode(map[string]string{"error": "empty request body"})
			return
		}

		objectKey := time.Now().UTC().Format("20060102150405") + "-et-sub.xml"
		ctx, cancel := context.WithTimeout(request.Context(), 2*time.Minute)
		defer cancel()

		if err := storage.UploadXML(ctx, objectKey, payload); err != nil {
			state.MarkFailure(err.Error())
			log.Printf("subscription callback upload failed: %v", err)
			writer.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(writer).Encode(map[string]string{"error": err.Error()})
			return
		}

		state.MarkSuccess(objectKey)
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]string{"status": "ok", "objectKey": objectKey})
	})

	go func() {
		log.Printf("health server listening on %s", addr)
		err := http.ListenAndServe(addr, mux)
		if err != nil {
			log.Fatalf("health server stopped: %v", err)
		}
	}()
}

func apiKeyMiddleware(apiKeys []string) func(http.Handler) http.Handler {
	allowed := make(map[string]struct{}, len(apiKeys))
	for _, key := range apiKeys {
		trimmed := strings.TrimSpace(key)
		if trimmed != "" {
			allowed[trimmed] = struct{}{}
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if len(allowed) == 0 {
				next.ServeHTTP(writer, request)
				return
			}

			provided := strings.TrimSpace(request.Header.Get("X-API-Key"))
			if provided == "" {
				authorization := strings.TrimSpace(request.Header.Get("Authorization"))
				if strings.HasPrefix(strings.ToLower(authorization), "bearer ") {
					provided = strings.TrimSpace(authorization[7:])
				}
			}

			if _, ok := allowed[provided]; !ok {
				writer.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(writer).Encode(map[string]string{"error": "unauthorized"})
				return
			}

			next.ServeHTTP(writer, request)
		})
	}
}

func usedFilesCount(usedFiles []s3store.UsedFile) int {
	return len(usedFiles)
}
