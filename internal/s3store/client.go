package s3store

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"ti1s3/internal/config"
)

type Client struct {
	httpClient *http.Client
	cfg        config.Config
}

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

func (client *Client) DeleteExpiredObjects(ctx context.Context, cutoff time.Time) error {
	continuationToken := ""
	totalScanned := 0
	totalDeleted := 0

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
			if object.LastModified.After(cutoff) || object.LastModified.Equal(cutoff) {
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

	log.Printf("retention cleanup complete: scanned=%d deleted=%d cutoff=%s", totalScanned, totalDeleted, cutoff.Format(time.RFC3339))
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
