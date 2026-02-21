package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type appConfig struct {
	EnturBaseURL string
	PollInterval time.Duration
	RequestorID  string

	S3Endpoint  string
	S3Region    string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3PathStyle bool

	HealthAddr string
}

type healthState struct {
	ok                atomic.Bool
	lastSuccessUnix   atomic.Int64
	lastSuccessObject atomic.Value
}

func main() {
	_ = loadDotEnvFile(".env")

	startupTimestamp := time.Now().Format("20060102T150405")
	cfg, err := loadConfig(startupTimestamp)
	if err != nil {
		log.Fatal(err)
	}

	httpClient := &http.Client{Timeout: 180 * time.Second}
	state := &healthState{}
	startHealthServer(cfg, state)
	cfg.PollInterval = 20 * time.Second

	log.Printf("starting poller with requestorId=%s interval=%s", cfg.RequestorID, cfg.PollInterval)
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	uploadOnce := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		xmlData, err := fetchEnturXML(ctx, httpClient, cfg.EnturBaseURL, cfg.RequestorID)
		if err != nil {
			state.ok.Store(false)
			log.Printf("fetch failed: %v", err)
			return
		}

		objectKey := time.Now().UTC().Format("20060102150405") + "-et.xml"
		uploadStartedAt := time.Now()
		if err := uploadToS3(ctx, httpClient, cfg, objectKey, xmlData); err != nil {
			state.ok.Store(false)
			log.Printf("upload failed after %s: %v", time.Since(uploadStartedAt).Round(time.Millisecond), err)
			return
		}

		nowUnix := time.Now().Unix()
		state.ok.Store(true)
		state.lastSuccessUnix.Store(nowUnix)
		state.lastSuccessObject.Store(objectKey)

		log.Printf("uploaded %s (%d bytes) in %s", objectKey, len(xmlData), time.Since(uploadStartedAt).Round(time.Millisecond))
	}

	uploadOnce()
	for range ticker.C {
		uploadOnce()
	}
}

func loadConfig(startupTimestamp string) (appConfig, error) {
	pollEverySeconds := getEnvInt("POLL_INTERVAL_SECONDS", 20)
	if pollEverySeconds < 1 {
		pollEverySeconds = 20
	}

	pathStyle := getEnvBool("S3_PATH_STYLE", true)
	requestorID := getEnv("ENTUR_REQUESTOR_ID", "ti1s3-"+startupTimestamp)

	cfg := appConfig{
		EnturBaseURL: getEnv("ENTUR_BASE_URL", "https://api.entur.io/realtime/v1/rest/et"),
		PollInterval: time.Duration(pollEverySeconds) * time.Second,
		RequestorID:  requestorID,
		S3Endpoint:   strings.TrimSpace(os.Getenv("S3_ENDPOINT")),
		S3Region:     getEnv("S3_REGION", "ume1"),
		S3Bucket:     strings.TrimSpace(os.Getenv("S3_BUCKET")),
		S3AccessKey:  strings.TrimSpace(os.Getenv("S3_ACCESS_KEY")),
		S3SecretKey:  strings.TrimSpace(os.Getenv("S3_SECRET_KEY")),
		S3PathStyle:  pathStyle,
		HealthAddr:   getEnv("HEALTH_ADDR", ":8080"),
	}

	if cfg.S3Endpoint == "" || cfg.S3Bucket == "" || cfg.S3AccessKey == "" || cfg.S3SecretKey == "" {
		return appConfig{}, fmt.Errorf("missing required S3 settings: S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY")
	}

	return cfg, nil
}

func startHealthServer(cfg appConfig, state *healthState) {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		ok := state.ok.Load()
		statusText := "not ok"
		statusCode := http.StatusNotFound
		if ok {
			statusText = "ok"
			statusCode = http.StatusOK
		}

		lastSuccessUnix := state.lastSuccessUnix.Load()
		lastSuccessRFC3339 := ""
		if lastSuccessUnix > 0 {
			lastSuccessRFC3339 = time.Unix(lastSuccessUnix, 0).UTC().Format(time.RFC3339)
		}

		lastObject := ""
		if value := state.lastSuccessObject.Load(); value != nil {
			if asString, ok := value.(string); ok {
				lastObject = asString
			}
		}

		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(statusCode)
		_ = json.NewEncoder(writer).Encode(map[string]interface{}{
			"status":             statusText,
			"requestorId":        cfg.RequestorID,
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
		log.Printf("health server listening on %s", cfg.HealthAddr)
		err := http.ListenAndServe(cfg.HealthAddr, mux)
		if err != nil {
			log.Fatalf("health server stopped: %v", err)
		}
	}()
}

func fetchEnturXML(ctx context.Context, client *http.Client, baseURL, requestorID string) ([]byte, error) {
	url := fmt.Sprintf("%s?useOriginalId=true&maxSize=100000&requestorId=%s", strings.TrimRight(baseURL, "/"), requestorID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("entur returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	return io.ReadAll(resp.Body)
}

func uploadToS3(ctx context.Context, client *http.Client, cfg appConfig, key string, data []byte) error {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	targetURL, host, canonicalURI, err := s3RequestTarget(cfg, key)
	if err != nil {
		return err
	}

	payloadHash := sha256Hex(data)
	canonicalHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	canonicalRequest := strings.Join([]string{
		http.MethodPut,
		canonicalURI,
		"",
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	credentialScope := dateStamp + "/" + cfg.S3Region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	signingKey := getSignatureKey(cfg.S3SecretKey, dateStamp, cfg.S3Region, "s3")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	authorization := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		cfg.S3AccessKey,
		credentialScope,
		signedHeaders,
		signature,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.Header.Set("Host", host)
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("Authorization", authorization)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("s3 returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	return nil
}

func s3RequestTarget(cfg appConfig, key string) (targetURL, host, canonicalURI string, err error) {
	endpoint := strings.TrimSpace(cfg.S3Endpoint)
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", "", "", fmt.Errorf("invalid S3_ENDPOINT: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", "", "", fmt.Errorf("invalid S3_ENDPOINT: must include scheme and host")
	}

	escapedKey := escapePathSegments(key)
	if cfg.S3PathStyle {
		host = u.Host
		canonicalURI = path.Clean("/" + cfg.S3Bucket + "/" + escapedKey)
	} else {
		host = cfg.S3Bucket + "." + u.Host
		canonicalURI = path.Clean("/" + escapedKey)
	}

	if !strings.HasPrefix(canonicalURI, "/") {
		canonicalURI = "/" + canonicalURI
	}

	targetURL = u.Scheme + "://" + host + canonicalURI
	return targetURL, host, canonicalURI, nil
}

func escapePathSegments(key string) string {
	parts := strings.Split(key, "/")
	for index, segment := range parts {
		parts[index] = url.PathEscape(segment)
	}
	return strings.Join(parts, "/")
}

func loadDotEnvFile(filePath string) error {
	content, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		parts := strings.SplitN(trimmed, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		value = strings.Trim(value, "\"")

		if key != "" {
			_ = os.Setenv(key, value)
		}
	}

	return nil
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func getSignatureKey(secret, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}

func getEnv(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func getEnvInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getEnvBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
