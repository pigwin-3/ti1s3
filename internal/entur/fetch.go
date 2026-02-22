package entur

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func FetchXML(ctx context.Context, client *http.Client, baseURL string, requestorID string) ([]byte, error) {
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
