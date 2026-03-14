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
	EnturSubscribeURL string
	PollInterval      time.Duration
	RequestorID       string
	Mode              string

	SubscribeEnabled                bool
	SubscribeConsumerAddress        string
	SubscribeCallbackPath           string
	SubscribeHeartbeatInterval      time.Duration
	SubscribeInitialTermination     time.Duration
	SubscribeUpdateInterval         time.Duration
	SubscribeAutoRenew              bool
	SubscribeRenewBeforeTermination time.Duration
	SubscribeDatasetID              string
	RetentionTTL                    time.Duration
	UsedRetentionTTL                time.Duration
	UsedFilesCacheTTL               time.Duration
	APIKeys                         []string

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
	mode := strings.ToLower(getEnv("ENTUR_MODE", "poll"))
	if mode != "poll" && mode != "subscribe" {
		mode = "poll"
	}

	subscribeEnabled := getEnvBool("ENTUR_SUBSCRIBE_ENABLED", mode == "subscribe")
	if mode == "subscribe" {
		subscribeEnabled = true
	}

	subscribeCallbackPath := strings.TrimSpace(getEnv("ENTUR_SUBSCRIBE_CALLBACK_PATH", "/entur/subscription"))
	if subscribeCallbackPath == "" {
		subscribeCallbackPath = "/entur/subscription"
	}
	if !strings.HasPrefix(subscribeCallbackPath, "/") {
		subscribeCallbackPath = "/" + subscribeCallbackPath
	}

	subscribeHeartbeatSeconds := getEnvInt("ENTUR_SUBSCRIBE_HEARTBEAT_SECONDS", 60)
	if subscribeHeartbeatSeconds < 1 {
		subscribeHeartbeatSeconds = 60
	}

	subscribeInitialTerminationMinutes := getEnvInt("ENTUR_SUBSCRIBE_INITIAL_TERMINATION_MINUTES", 60)
	if subscribeInitialTerminationMinutes < 1 {
		subscribeInitialTerminationMinutes = 60
	}

	subscribeUpdateIntervalSeconds := getEnvInt("ENTUR_SUBSCRIBE_UPDATE_INTERVAL_SECONDS", 0)
	if subscribeUpdateIntervalSeconds < 0 {
		subscribeUpdateIntervalSeconds = 0
	}

	subscribeRenewBeforeMinutes := getEnvInt("ENTUR_SUBSCRIBE_RENEW_BEFORE_MINUTES", 5)
	if subscribeRenewBeforeMinutes < 1 {
		subscribeRenewBeforeMinutes = 5
	}

	cfg := Config{
		EnturBaseURL:      getEnv("ENTUR_BASE_URL", "https://api.entur.io/realtime/v1/rest/et"),
		EnturSubscribeURL: getEnv("ENTUR_SUBSCRIBE_URL", "https://api.entur.io/realtime/v1/subscribe"),
		PollInterval:      time.Duration(pollEverySeconds) * time.Second,
		RequestorID:       requestorID,
		Mode:              mode,

		SubscribeEnabled:                subscribeEnabled,
		SubscribeConsumerAddress:        strings.TrimSpace(os.Getenv("ENTUR_SUBSCRIBE_CONSUMER_ADDRESS")),
		SubscribeCallbackPath:           subscribeCallbackPath,
		SubscribeHeartbeatInterval:      time.Duration(subscribeHeartbeatSeconds) * time.Second,
		SubscribeInitialTermination:     time.Duration(subscribeInitialTerminationMinutes) * time.Minute,
		SubscribeUpdateInterval:         time.Duration(subscribeUpdateIntervalSeconds) * time.Second,
		SubscribeAutoRenew:              getEnvBool("ENTUR_SUBSCRIBE_AUTO_RENEW", true),
		SubscribeRenewBeforeTermination: time.Duration(subscribeRenewBeforeMinutes) * time.Minute,
		SubscribeDatasetID:              strings.TrimSpace(os.Getenv("ENTUR_SUBSCRIBE_DATASET_ID")),
		RetentionTTL:                    time.Duration(retentionHours) * time.Hour,
		UsedRetentionTTL:                time.Duration(usedRetentionHours) * time.Hour,
		UsedFilesCacheTTL:               time.Duration(usedFilesCacheSeconds) * time.Second,
		APIKeys:                         apiKeys,
		S3Endpoint:                      strings.TrimSpace(os.Getenv("S3_ENDPOINT")),
		S3Region:                        getEnv("S3_REGION", "ume1"),
		S3Bucket:                        strings.TrimSpace(os.Getenv("S3_BUCKET")),
		S3AccessKey:                     strings.TrimSpace(os.Getenv("S3_ACCESS_KEY")),
		S3SecretKey:                     strings.TrimSpace(os.Getenv("S3_SECRET_KEY")),
		S3PathStyle:                     pathStyle,
		HealthAddr:                      getEnv("HEALTH_ADDR", ":8080"),
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
