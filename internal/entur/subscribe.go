package entur

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"ti1s3/internal/config"
)

type SubscribeResult struct {
	StatusCode  int
	Body        string
	NextRenewAt time.Time
}

func SubscribeET(ctx context.Context, client *http.Client, cfg config.Config) (SubscribeResult, error) {
	now := time.Now().UTC()
	messageID := fmt.Sprintf("%s-sub-%d", cfg.RequestorID, now.UnixNano())
	termination := now.Add(cfg.SubscribeInitialTermination)

	body := buildSubscribeRequestBody(
		now,
		cfg.RequestorID,
		cfg.SubscribeConsumerAddress,
		messageID,
		termination,
		cfg.SubscribeHeartbeatInterval,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSpace(cfg.EnturSubscribeURL), strings.NewReader(body))
	if err != nil {
		return SubscribeResult{}, err
	}
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("Accept", "application/xml, text/xml, application/json")

	resp, err := client.Do(req)
	if err != nil {
		return SubscribeResult{}, err
	}
	defer resp.Body.Close()

	responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	result := SubscribeResult{
		StatusCode: resp.StatusCode,
		Body:       strings.TrimSpace(string(responseBody)),
	}

	renewAt := termination.Add(-cfg.SubscribeRenewBeforeTermination)
	if renewAt.Before(now.Add(30 * time.Second)) {
		renewAt = now.Add(30 * time.Second)
	}
	result.NextRenewAt = renewAt

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if result.Body == "" {
			return result, fmt.Errorf("subscribe failed with %s", resp.Status)
		}
		return result, fmt.Errorf("subscribe failed with %s: %s", resp.Status, result.Body)
	}

	return result, nil
}

func buildSubscribeRequestBody(
	now time.Time,
	requestorID string,
	consumerAddress string,
	messageID string,
	initialTerminationTime time.Time,
	heartbeatInterval time.Duration,
) string {
	requestTimestamp := now.Format(time.RFC3339)
	terminationTimestamp := initialTerminationTime.Format(time.RFC3339)
	heartbeat := formatDurationSeconds(heartbeatInterval)

	var b bytes.Buffer
	b.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	b.WriteString("<Siri version=\"2.0\" xmlns=\"http://www.siri.org.uk/siri\">\n")
	b.WriteString("  <ServiceRequest>\n")
	b.WriteString("    <RequestTimestamp>")
	b.WriteString(requestTimestamp)
	b.WriteString("</RequestTimestamp>\n")
	b.WriteString("    <RequestorRef>")
	b.WriteString(xmlEscape(requestorID))
	b.WriteString("</RequestorRef>\n")
	b.WriteString("    <SubscribeRequest>\n")
	b.WriteString("      <RequestTimestamp>")
	b.WriteString(requestTimestamp)
	b.WriteString("</RequestTimestamp>\n")
	b.WriteString("      <MessageIdentifier>")
	b.WriteString(xmlEscape(messageID))
	b.WriteString("</MessageIdentifier>\n")
	b.WriteString("      <ConsumerAddress>")
	b.WriteString(xmlEscape(consumerAddress))
	b.WriteString("</ConsumerAddress>\n")
	b.WriteString("      <InitialTerminationTime>")
	b.WriteString(terminationTimestamp)
	b.WriteString("</InitialTerminationTime>\n")
	b.WriteString("      <HeartbeatInterval>")
	b.WriteString(heartbeat)
	b.WriteString("</HeartbeatInterval>\n")
	b.WriteString("      <EstimatedTimetableSubscriptionRequest>\n")
	b.WriteString("        <SubscriptionIdentifier>")
	b.WriteString(xmlEscape(messageID))
	b.WriteString("</SubscriptionIdentifier>\n")
	b.WriteString("      </EstimatedTimetableSubscriptionRequest>\n")
	b.WriteString("    </SubscribeRequest>\n")
	b.WriteString("  </ServiceRequest>\n")
	b.WriteString("</Siri>")

	return b.String()
}

func xmlEscape(value string) string {
	replacer := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		"\"", "&quot;",
		"'", "&apos;",
	)
	return replacer.Replace(value)
}

func formatDurationSeconds(duration time.Duration) string {
	seconds := int(duration.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	return fmt.Sprintf("PT%dS", seconds)
}
