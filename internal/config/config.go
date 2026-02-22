package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	EnturBaseURL      string
	PollInterval      time.Duration
	RequestorID       string
	RetentionTTL      time.Duration
	UsedRetentionTTL  time.Duration
	UsedFilesCacheTTL time.Duration
	APIKeys           []string

	S3Endpoint  string
	S3Region    string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3PathStyle bool

	HealthAddr string
}

func Load(startupTimestamp string) (Config, error) {
	pollEverySeconds := getEnvInt("POLL_INTERVAL_SECONDS", 20)
	if pollEverySeconds < 1 {
		pollEverySeconds = 20
	}

	retentionHours := getEnvInt("RETENTION_HOURS", 24*7)
	if retentionHours < 1 {
		retentionHours = 24 * 7
	}

	usedRetentionHours := retentionHours
	usedRetentionRaw := strings.TrimSpace(os.Getenv("USED_RETENTION_HOURS"))
	if usedRetentionRaw != "" {
		parsed, err := strconv.Atoi(usedRetentionRaw)
		if err == nil && parsed > 0 {
			usedRetentionHours = parsed
		}
	}

	usedFilesCacheSeconds := getEnvInt("USED_FILES_CACHE_SECONDS", 300)
	if usedFilesCacheSeconds < 1 {
		usedFilesCacheSeconds = 300
	}

	pathStyle := getEnvBool("S3_PATH_STYLE", true)
	requestorID := getEnv("ENTUR_REQUESTOR_ID", "ti1s3-"+startupTimestamp)
	apiKeys := getEnvCSV("API_KEYS")

	cfg := Config{
		EnturBaseURL:      getEnv("ENTUR_BASE_URL", "https://api.entur.io/realtime/v1/rest/et"),
		PollInterval:      time.Duration(pollEverySeconds) * time.Second,
		RequestorID:       requestorID,
		RetentionTTL:      time.Duration(retentionHours) * time.Hour,
		UsedRetentionTTL:  time.Duration(usedRetentionHours) * time.Hour,
		UsedFilesCacheTTL: time.Duration(usedFilesCacheSeconds) * time.Second,
		APIKeys:           apiKeys,
		S3Endpoint:        strings.TrimSpace(os.Getenv("S3_ENDPOINT")),
		S3Region:          getEnv("S3_REGION", "ume1"),
		S3Bucket:          strings.TrimSpace(os.Getenv("S3_BUCKET")),
		S3AccessKey:       strings.TrimSpace(os.Getenv("S3_ACCESS_KEY")),
		S3SecretKey:       strings.TrimSpace(os.Getenv("S3_SECRET_KEY")),
		S3PathStyle:       pathStyle,
		HealthAddr:        getEnv("HEALTH_ADDR", ":8080"),
	}

	if cfg.S3Endpoint == "" || cfg.S3Bucket == "" || cfg.S3AccessKey == "" || cfg.S3SecretKey == "" {
		return Config{}, fmt.Errorf("missing required S3 settings: S3_ENDPOINT, S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY")
	}

	return cfg, nil
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

func getEnvCSV(key string) []string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return nil
	}

	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}
