package health

import (
	"encoding/json"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

type State struct {
	ok                atomic.Bool
	lastSuccessUnix   atomic.Int64
	lastSuccessObject atomic.Value
}

func NewState() *State {
	return &State{}
}

func (state *State) MarkFailure() {
	state.ok.Store(false)
}

func (state *State) MarkSuccess(objectKey string) {
	state.ok.Store(true)
	state.lastSuccessUnix.Store(time.Now().Unix())
	state.lastSuccessObject.Store(objectKey)
}

func (state *State) Snapshot() (bool, int64, string, string) {
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

	return ok, lastSuccessUnix, lastObject, lastSuccessRFC3339
}

func StartServer(addr string, requestorID string, state *State) {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		ok, lastSuccessUnix, lastObject, lastSuccessRFC3339 := state.Snapshot()
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
			"requestorId":        requestorID,
			"lastSuccessUnix":    lastSuccessUnix,
			"lastSuccessRFC3339": lastSuccessRFC3339,
			"lastSuccessObject":  lastObject,
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

	go func() {
		log.Printf("health server listening on %s", addr)
		err := http.ListenAndServe(addr, mux)
		if err != nil {
			log.Fatalf("health server stopped: %v", err)
		}
	}()
}
