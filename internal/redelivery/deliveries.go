// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package redelivery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Delivery describes an organization webhook delivery.
type Delivery struct {
	DeliveredAt time.Time `json:"delivered_at"` //nolint:tagliatelle // GitHub names this field delivered_at.
	GUID        string    `json:"guid"`
	Status      string    `json:"status"`
	ID          int64     `json:"id"`
	StatusCode  int       `json:"status_code"` //nolint:tagliatelle // GitHub names this field status_code.
	Redelivery  bool      `json:"redelivery"`
}

// AuthError reports rejected credentials or a failed token exchange.
type AuthError struct{ error }

// TransientError reports a transport or GitHub server failure.
type TransientError struct{ error }

// RateLimitError reports a rate limit and how long GitHub asks clients to wait.
type RateLimitError struct {
	error

	Wait time.Duration
}

var (
	controlCharacters = regexp.MustCompile(`\p{Cc}`)
	nextLink          = regexp.MustCompile(`(?:^|,)\s*<([^<>]+)>\s*(?:;\s*[^;,]+)*?;\s*rel="next"\s*(?:[,;]|$)`)
)

// ListDeliveries returns one page in GitHub's newest-first order and the cursor
// for the next page. A missing or malformed next link yields an empty cursor.
//
//nolint:err113 // List diagnostics include the endpoint and status without decoder details.
func (c *Client) ListDeliveries(ctx context.Context, cursor string) ([]Delivery, string, error) {
	query := url.Values{"per_page": {"100"}}
	if cursor != "" {
		query.Set("cursor", cursor)
	}

	res, err := c.deliveryRequest(ctx, http.MethodGet, c.deliveriesPath, "?"+query.Encode(), http.StatusOK)
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()

	data, err := io.ReadAll(io.LimitReader(res.Body, responseLimit+1))
	if err != nil {
		return nil, "", &TransientError{fmt.Errorf("GET %s: HTTP %d read response", c.deliveriesPath, res.StatusCode)}
	}

	var page []Delivery
	if len(data) > responseLimit || json.Unmarshal(data, &page) != nil {
		return nil, "", fmt.Errorf("GET %s: HTTP %d invalid delivery response", c.deliveriesPath, res.StatusCode)
	}

	return page, nextDeliveryCursor(res.Header), nil
}

// Redeliver requests another attempt for the specified delivery.
func (c *Client) Redeliver(ctx context.Context, deliveryID int64) error {
	path := fmt.Sprintf("%s/%d/attempts", c.deliveriesPath, deliveryID)

	res, err := c.deliveryRequest(ctx, http.MethodPost, path, "", http.StatusAccepted)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return nil
}

func nextDeliveryCursor(header http.Header) string {
	match := nextLink.FindStringSubmatch(strings.Join(header.Values("Link"), ","))
	if match == nil {
		return ""
	}

	link, err := url.Parse(match[1])
	if err != nil {
		return ""
	}

	return link.Query().Get("cursor")
}

//nolint:err113 // Request diagnostics omit URL and transport errors that can contain credentials.
func (c *Client) deliveryRequest(
	ctx context.Context, method, path, query string, want int,
) (*http.Response, error) {
	var tokens []string

	for retry := true; ; retry = false {
		token, err := c.Token(ctx)
		if err != nil {
			return nil, &AuthError{err}
		}

		tokens = append(tokens, token)

		req, err := http.NewRequestWithContext(ctx, method, c.options.BaseURL+path+query, nil)
		if err != nil {
			return nil, fmt.Errorf("%s %s: create request", method, path)
		}

		setGitHubHeaders(req, token)

		res, err := c.options.HTTPClient.Do(req)
		if err != nil {
			return nil, &TransientError{fmt.Errorf("%s %s: send request", method, path)}
		}

		if res.StatusCode == want {
			return res, nil
		}

		if res.StatusCode == http.StatusUnauthorized && retry {
			//nolint:gosec // The rejected response body is discarded.
			res.Body.Close()
			c.Invalidate()

			continue
		}

		return nil, c.deliveryError(method, path, res, tokens)
	}
}

//nolint:err113 // HTTP diagnostics include the endpoint, status and sanitized message.
func (c *Client) deliveryError(method, path string, res *http.Response, tokens []string) error {
	defer res.Body.Close()

	err := fmt.Errorf("%s %s: HTTP %d%s", method, path, res.StatusCode, errorMessage(res.Body, tokens))

	const serverErrorLimit = 600

	status := res.StatusCode
	switch {
	case (status == http.StatusForbidden || status == http.StatusTooManyRequests) &&
		(res.Header.Get("X-Ratelimit-Remaining") == "0" || len(res.Header.Values("Retry-After")) != 0):
		return &RateLimitError{err, retryWait(res.Header, c.options.Now())}
	case status == http.StatusForbidden || status == http.StatusUnauthorized:
		return &AuthError{err}
	case status >= http.StatusInternalServerError && status < serverErrorLimit:
		return &TransientError{err}
	}

	return err
}

func errorMessage(body io.Reader, tokens []string) string {
	var payload struct {
		Message string `json:"message"`
	}
	if json.NewDecoder(io.LimitReader(body, responseLimit)).Decode(&payload) != nil {
		return ""
	}

	message := controlCharacters.ReplaceAllString(payload.Message, "")
	for _, token := range tokens {
		if strings.Contains(message, token) {
			return ""
		}
	}

	const maxMessageBytes = 200

	message = strings.ToValidUTF8(message[:min(len(message), maxMessageBytes)], "")
	if message == "" {
		return ""
	}

	return ": " + message
}

func retryWait(header http.Header, now time.Time) time.Duration {
	const maxWait = time.Hour

	seconds, err := strconv.ParseInt(header.Get("Retry-After"), 10, 64)
	if err == nil {
		return time.Duration(min(max(seconds, 0), int64(maxWait/time.Second))) * time.Second
	}

	reset, err := strconv.ParseInt(header.Get("X-Ratelimit-Reset"), 10, 64)
	if err != nil {
		return 0
	}

	return min(max(time.Unix(reset, 0).Sub(now), 0), maxWait)
}
