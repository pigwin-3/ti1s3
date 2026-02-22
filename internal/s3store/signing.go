package s3store

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"ti1s3/internal/config"
)

func doSignedRequest(
	ctx context.Context,
	httpClient *http.Client,
	cfg config.Config,
	method string,
	targetURL string,
	host string,
	canonicalURI string,
	canonicalQuery string,
	contentType string,
	body []byte,
) (*http.Response, error) {
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	payloadHash := sha256Hex(body)
	canonicalHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"

	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQuery,
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

	signingKey := signatureKey(cfg.S3SecretKey, dateStamp, cfg.S3Region, "s3")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	authorization := fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		cfg.S3AccessKey,
		credentialScope,
		signedHeaders,
		signature,
	)

	req, err := http.NewRequestWithContext(ctx, method, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Host", host)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("Authorization", authorization)

	return httpClient.Do(req)
}

func requestTarget(cfg config.Config, key string) (targetURL string, host string, canonicalURI string, err error) {
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

func canonicalQueryString(values url.Values) string {
	if len(values) == 0 {
		return ""
	}

	type keyValue struct {
		key   string
		value string
	}

	pairs := make([]keyValue, 0)
	for key, allValues := range values {
		if len(allValues) == 0 {
			pairs = append(pairs, keyValue{key: key, value: ""})
			continue
		}
		for _, value := range allValues {
			pairs = append(pairs, keyValue{key: key, value: value})
		}
	}

	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key == pairs[j].key {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].key < pairs[j].key
	})

	builder := strings.Builder{}
	for index, pair := range pairs {
		if index > 0 {
			builder.WriteByte('&')
		}
		builder.WriteString(awsPercentEncode(pair.key))
		builder.WriteByte('=')
		builder.WriteString(awsPercentEncode(pair.value))
	}

	return builder.String()
}

func escapePathSegments(key string) string {
	parts := strings.Split(key, "/")
	for index, segment := range parts {
		parts[index] = url.PathEscape(segment)
	}
	return strings.Join(parts, "/")
}

func awsPercentEncode(value string) string {
	encoded := url.QueryEscape(value)
	encoded = strings.ReplaceAll(encoded, "+", "%20")
	encoded = strings.ReplaceAll(encoded, "*", "%2A")
	encoded = strings.ReplaceAll(encoded, "%7E", "~")
	return encoded
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

func signatureKey(secret string, dateStamp string, region string, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	return hmacSHA256(kService, "aws4_request")
}
