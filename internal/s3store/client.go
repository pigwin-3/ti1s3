package s3store

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"ti1s3/internal/config"
)

type Client struct {
	httpClient *http.Client
	cfg        config.Config

	mu              sync.RWMutex
	usedFilesCache  map[string]time.Time
	usedFilesCached time.Time
}

type UsedFile struct {
	Key    string    `json:"key"`
	UsedAt time.Time `json:"usedAt"`
}

type ErrorLogEntry struct {
	OccurredAt time.Time `json:"occurredAt"`
	Message    string    `json:"message"`
}

type usedFileIndex struct {
	Files map[string]time.Time `json:"files"`
}

type errorLogFile struct {
	Date   string          `json:"date"`
	Errors []ErrorLogEntry `json:"errors"`
}

const usedFilesIndexKey = "_meta/used-files.json"
const errorLogsPrefix = "_meta/logs/"

func NewClient(httpClient *http.Client, cfg config.Config) *Client {
	return &Client{httpClient: httpClient, cfg: cfg}
}

func (client *Client) UploadXML(ctx context.Context, key string, data []byte) error {
	targetURL, host, canonicalURI, err := requestTarget(client.cfg, key)
	if err != nil {
		return err
	}

	resp, err := doSignedRequest(ctx, client.httpClient, client.cfg, http.MethodPut, targetURL, host, canonicalURI, "", "application/xml", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("s3 returned %s: %s", resp.Status, body)
	}

	return nil
}

func (client *Client) DeleteExpiredObjects(ctx context.Context, cutoff time.Time, usedCutoff time.Time, usedFiles map[string]time.Time) error {
	continuationToken := ""
	totalScanned := 0
	totalDeleted := 0
	remainingKeys := make(map[string]struct{})

	for {
		listing, err := client.listObjectsPage(ctx, continuationToken)
		if err != nil {
			return err
		}

		for _, object := range listing.Contents {
			totalScanned++
			if object.Key == "" {
				continue
			}
			if object.Key == usedFilesIndexKey {
				continue
			}

			objectCutoff := cutoff
			if _, isUsed := usedFiles[object.Key]; isUsed {
				objectCutoff = usedCutoff
			}

			if object.LastModified.After(objectCutoff) || object.LastModified.Equal(objectCutoff) {
				remainingKeys[object.Key] = struct{}{}
				continue
			}

			if err := client.deleteObject(ctx, object.Key); err != nil {
				return fmt.Errorf("delete %q: %w", object.Key, err)
			}
			totalDeleted++
		}

		if !listing.IsTruncated || listing.NextContinuationToken == "" {
			break
		}
		continuationToken = listing.NextContinuationToken
	}

	if err := client.pruneUsedFilesIndex(ctx, usedFiles, remainingKeys); err != nil {
		return err
	}

	log.Printf("retention cleanup complete: scanned=%d deleted=%d cutoff_default=%s cutoff_used=%s", totalScanned, totalDeleted, cutoff.Format(time.RFC3339), usedCutoff.Format(time.RFC3339))
	return nil
}

func (client *Client) ListUsedFiles(ctx context.Context) ([]UsedFile, error) {
	index, err := client.loadUsedFileIndex(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]UsedFile, 0, len(index.Files))
	for key, usedAt := range index.Files {
		result = append(result, UsedFile{Key: key, UsedAt: usedAt.UTC()})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].UsedAt.After(result[j].UsedAt)
	})

	return result, nil
}

func (client *Client) UsedFilesSet(ctx context.Context) (map[string]time.Time, error) {
	if cached := client.getUsedFilesCache(); cached != nil {
		return cached, nil
	}

	index, err := client.loadUsedFileIndex(ctx)
	if err != nil {
		return nil, err
	}

	client.setUsedFilesCache(index.Files)
	return copyUsedFilesMap(index.Files), nil
}

func (client *Client) MarkFileUsed(ctx context.Context, key string, usedAt time.Time) error {
	if key == "" {
		return fmt.Errorf("key is required")
	}

	usedFiles, err := client.UsedFilesSet(ctx)
	if err != nil {
		return err
	}

	if usedFiles == nil {
		usedFiles = make(map[string]time.Time)
	}

	usedFiles[key] = usedAt.UTC()
	if err := client.saveUsedFileIndex(ctx, usedFileIndex{Files: usedFiles}); err != nil {
		return err
	}

	client.setUsedFilesCache(usedFiles)
	return nil
}

func (client *Client) AppendErrorLog(ctx context.Context, message string, occurredAt time.Time) error {
	trimmed := strings.TrimSpace(message)
	if trimmed == "" {
		return fmt.Errorf("message is required")
	}

	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	} else {
		occurredAt = occurredAt.UTC()
	}

	day := occurredAt.Format("2006-01-02")
	key := errorLogObjectKey(occurredAt)

	current, err := client.loadErrorLogFile(ctx, key)
	if err != nil {
		return err
	}

	current.Date = day
	current.Errors = append(current.Errors, ErrorLogEntry{
		OccurredAt: occurredAt,
		Message:    trimmed,
	})

	if err := client.saveErrorLogFile(ctx, key, current); err != nil {
		return err
	}

	return nil
}

func (client *Client) loadUsedFileIndex(ctx context.Context) (usedFileIndex, error) {
	targetURL, host, canonicalURI, err := requestTarget(client.cfg, usedFilesIndexKey)
	if err != nil {
		return usedFileIndex{}, err
	}

	resp, err := doSignedRequest(ctx, client.httpClient, client.cfg, http.MethodGet, targetURL, host, canonicalURI, "", "", nil)
	if err != nil {
		return usedFileIndex{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return usedFileIndex{Files: map[string]time.Time{}}, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return usedFileIndex{}, fmt.Errorf("s3 returned %s: %s", resp.Status, body)
	}

	var index usedFileIndex
	if err := json.NewDecoder(resp.Body).Decode(&index); err != nil {
		return usedFileIndex{}, fmt.Errorf("decode used file index: %w", err)
	}

	if index.Files == nil {
		index.Files = map[string]time.Time{}
	}

	return index, nil
}

func (client *Client) pruneUsedFilesIndex(ctx context.Context, currentUsedFiles map[string]time.Time, remainingKeys map[string]struct{}) error {
	if len(currentUsedFiles) == 0 {
		return nil
	}

	pruned := make(map[string]time.Time, len(currentUsedFiles))
	changed := false

	for key, usedAt := range currentUsedFiles {
		if _, exists := remainingKeys[key]; exists {
			pruned[key] = usedAt
			continue
		}
		changed = true
	}

	if !changed {
		client.setUsedFilesCache(currentUsedFiles)
		return nil
	}

	if err := client.saveUsedFileIndex(ctx, usedFileIndex{Files: pruned}); err != nil {
		return fmt.Errorf("prune used file index: %w", err)
	}

	client.setUsedFilesCache(pruned)
	return nil
}

func (client *Client) getUsedFilesCache() map[string]time.Time {
	client.mu.RLock()
	defer client.mu.RUnlock()

	if client.usedFilesCache == nil {
		return nil
	}

	cacheTTL := client.cfg.UsedFilesCacheTTL
	if cacheTTL <= 0 {
		cacheTTL = 5 * time.Minute
	}

	if time.Since(client.usedFilesCached) > cacheTTL {
		return nil
	}

	return copyUsedFilesMap(client.usedFilesCache)
}

func (client *Client) setUsedFilesCache(usedFiles map[string]time.Time) {
	client.mu.Lock()
	defer client.mu.Unlock()

	client.usedFilesCache = copyUsedFilesMap(usedFiles)
	client.usedFilesCached = time.Now()
}

func copyUsedFilesMap(source map[string]time.Time) map[string]time.Time {
	if source == nil {
		return nil
	}

	result := make(map[string]time.Time, len(source))
	for key, value := range source {
		result[key] = value
	}

	return result
}

func errorLogObjectKey(occurredAt time.Time) string {
	return errorLogsPrefix + occurredAt.UTC().Format("2006-01-02") + ".json"
}

func (client *Client) saveUsedFileIndex(ctx context.Context, index usedFileIndex) error {
	data, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("encode used file index: %w", err)
	}

	targetURL, host, canonicalURI, err := requestTarget(client.cfg, usedFilesIndexKey)
	if err != nil {
		return err
	}

	resp, err := doSignedRequest(ctx, client.httpClient, client.cfg, http.MethodPut, targetURL, host, canonicalURI, "", "application/json", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("s3 returned %s: %s", resp.Status, body)
	}

	return nil
}

func (client *Client) loadErrorLogFile(ctx context.Context, key string) (errorLogFile, error) {
	targetURL, host, canonicalURI, err := requestTarget(client.cfg, key)
	if err != nil {
		return errorLogFile{}, err
	}

	resp, err := doSignedRequest(ctx, client.httpClient, client.cfg, http.MethodGet, targetURL, host, canonicalURI, "", "", nil)
	if err != nil {
		return errorLogFile{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return errorLogFile{Errors: []ErrorLogEntry{}}, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return errorLogFile{}, fmt.Errorf("s3 returned %s: %s", resp.Status, body)
	}

	var logFile errorLogFile
	if err := json.NewDecoder(resp.Body).Decode(&logFile); err != nil {
		return errorLogFile{}, fmt.Errorf("decode error log file: %w", err)
	}

	if logFile.Errors == nil {
		logFile.Errors = []ErrorLogEntry{}
	}

	return logFile, nil
}

func (client *Client) saveErrorLogFile(ctx context.Context, key string, logFile errorLogFile) error {
	data, err := json.Marshal(logFile)
	if err != nil {
		return fmt.Errorf("encode error log file: %w", err)
	}

	targetURL, host, canonicalURI, err := requestTarget(client.cfg, key)
	if err != nil {
		return err
	}

	resp, err := doSignedRequest(ctx, client.httpClient, client.cfg, http.MethodPut, targetURL, host, canonicalURI, "", "application/json", data)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("s3 returned %s: %s", resp.Status, body)
	}

	return nil
}

func (client *Client) listObjectsPage(ctx context.Context, continuationToken string) (listBucketResult, error) {
	targetURL, host, canonicalURI, err := requestTarget(client.cfg, "")
	if err != nil {
		return listBucketResult{}, err
	}

	queryValues := url.Values{}
	queryValues.Set("list-type", "2")
	if continuationToken != "" {
		queryValues.Set("continuation-token", continuationToken)
	}

	canonicalQuery := canonicalQueryString(queryValues)
	if canonicalQuery != "" {
		targetURL = targetURL + "?" + canonicalQuery
	}

	resp, err := doSignedRequest(ctx, client.httpClient, client.cfg, http.MethodGet, targetURL, host, canonicalURI, canonicalQuery, "", nil)
	if err != nil {
		return listBucketResult{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return listBucketResult{}, fmt.Errorf("s3 returned %s: %s", resp.Status, body)
	}

	var listing listBucketResult
	if err := xml.NewDecoder(resp.Body).Decode(&listing); err != nil {
		return listBucketResult{}, fmt.Errorf("decode list response: %w", err)
	}

	return listing, nil
}

func (client *Client) deleteObject(ctx context.Context, key string) error {
	targetURL, host, canonicalURI, err := requestTarget(client.cfg, key)
	if err != nil {
		return err
	}

	resp, err := doSignedRequest(ctx, client.httpClient, client.cfg, http.MethodDelete, targetURL, host, canonicalURI, "", "", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("s3 returned %s: %s", resp.Status, body)
	}

	return nil
}

type listBucketResult struct {
	Contents              []s3Object `xml:"Contents"`
	IsTruncated           bool       `xml:"IsTruncated"`
	NextContinuationToken string     `xml:"NextContinuationToken"`
}

type s3Object struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
}
