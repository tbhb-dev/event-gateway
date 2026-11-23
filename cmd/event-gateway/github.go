// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const defaultBodyCap = 5 << 20

const maxGitHubBody = 25 << 20

const (
	webhookPath    = "/webhooks/github"
	healthPath     = "/healthz"
	eventHeader    = "X-Github-Event"
	deliveryHeader = "X-Github-Delivery"
)

var (
	githubEvent    = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	githubDelivery = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

//nolint:tagliatelle // The envelope uses the gateway protocol field names.
type envelope struct {
	Type       string  `json:"type"`
	Provider   string  `json:"provider"`
	Event      string  `json:"event"`
	Delivery   string  `json:"delivery"`
	Repository *string `json:"repository"`
	ReceivedAt string  `json:"received_at"`
	Payload    []byte  `json:"-"`
}

func (event envelope) encode() []byte {
	//nolint:errcheck,errchkjson // Envelope metadata contains only strings and null.
	data, _ := json.Marshal(event)
	// The payload is already JSON. Appending it preserves the signed bytes.
	data = append(data[:len(data)-1], `,"payload":`...)
	data = append(data, event.Payload...)

	return append(data, '}')
}

//nolint:tagliatelle // GitHub sends full_name.
type githubRepository struct {
	FullName *string `json:"full_name"`
}

type githubPayload struct {
	Repository githubRepository `json:"repository"`
}

// truncatedGitHub keeps filter fields at their original JSON paths.
//
//nolint:tagliatelle // GitHub sends these field names.
type truncatedGitHub struct {
	Action      *string            `json:"action,omitempty"`
	CheckRun    *githubCheck       `json:"check_run,omitempty"`
	CheckSuite  *githubCheck       `json:"check_suite,omitempty"`
	WorkflowRun *githubCheck       `json:"workflow_run,omitempty"`
	WorkflowJob *githubCheck       `json:"workflow_job,omitempty"`
	Issue       *githubItem        `json:"issue,omitempty"`
	PullRequest *githubPullRequest `json:"pull_request,omitempty"`
	Label       *githubLabel       `json:"label,omitempty"`
	Truncated   bool               `json:"truncated"`
}

type githubCheck struct {
	Name       *string `json:"name,omitempty"`
	Conclusion *string `json:"conclusion,omitempty"`
	Path       *string `json:"path,omitempty"`
}

type githubLabel struct {
	Name string `json:"name"`
}

type githubItem struct {
	Number *int64        `json:"number,omitempty"`
	Labels []githubLabel `json:"labels,omitempty"`
}

type githubPullRequest struct {
	Head   *githubHead   `json:"head,omitempty"`
	Number *int64        `json:"number,omitempty"`
	Labels []githubLabel `json:"labels,omitempty"`
}

type githubHead struct {
	SHA string `json:"sha"`
}

type githubWebhook struct {
	store   eventStore
	logger  *slog.Logger
	secret  []byte
	bodyCap int
}

func (h *githubWebhook) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	path := request.URL.EscapedPath()

	switch {
	case request.Method == http.MethodGet && path == healthPath:
		response.WriteHeader(http.StatusOK)
	case request.Method == http.MethodPost && path == webhookPath:
		response.WriteHeader(h.ingest(response, request))
	default:
		http.NotFound(response, request)
	}
}

func (h *githubWebhook) ingest(response http.ResponseWriter, request *http.Request) int {
	receivedAt := time.Now().UTC()
	signature := request.Header.Get("X-Hub-Signature-256")

	digest, err := decodeSignature(signature)
	if err != nil {
		return http.StatusUnauthorized
	}

	body, err := io.ReadAll(http.MaxBytesReader(response, request.Body, maxGitHubBody))
	if err != nil {
		_, tooLarge := errors.AsType[*http.MaxBytesError](err)
		if tooLarge {
			return http.StatusRequestEntityTooLarge
		}

		return http.StatusBadRequest
	}

	if !verifySignature(h.secret, body, digest) {
		return http.StatusUnauthorized
	}

	event := request.Header.Get(eventHeader)
	delivery := request.Header.Get(deliveryHeader)

	if !githubEvent.MatchString(event) || !githubDelivery.MatchString(delivery) {
		return http.StatusBadRequest
	}

	normalized, err := normalizeGitHub(event, delivery, receivedAt, body, h.bodyCap)
	if err != nil {
		return http.StatusBadRequest
	}

	if event == "ping" {
		return http.StatusAccepted
	}

	hash := sha256.Sum256(body)
	err = h.store.publish(request.Context(), normalized, hex.EncodeToString(hash[:]))

	return h.publishStatus(request.Context(), err)
}

func (h *githubWebhook) publishStatus(ctx context.Context, err error) int {
	switch {
	case err == nil:
		return http.StatusAccepted
	case errors.Is(err, errBodyConflict):
		h.logger.WarnContext(ctx, "GitHub delivery already has a different body")

		return http.StatusConflict
	default:
		h.logger.ErrorContext(ctx, "GitHub event storage unavailable")

		return http.StatusServiceUnavailable
	}
}

var errSignature = errors.New("invalid GitHub signature")

func decodeSignature(header string) ([]byte, error) {
	signature, ok := strings.CutPrefix(header, "sha256=")
	if !ok || len(signature) != hex.EncodedLen(sha256.Size) {
		return nil, errSignature
	}

	digest, err := hex.DecodeString(signature)
	if err != nil {
		return nil, errSignature
	}

	return digest, nil
}

func verifySignature(secret, body, signature []byte) bool {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)

	return hmac.Equal(mac.Sum(nil), signature)
}

func normalizeGitHub(event, delivery string, receivedAt time.Time, body []byte, bodyCap int) (envelope, error) {
	var scope githubPayload

	err := json.Unmarshal(body, &scope)
	if err != nil {
		return envelope{}, fmt.Errorf("decode GitHub payload: %w", err)
	}

	message := envelope{
		Type:       "event",
		Provider:   "github",
		Event:      event,
		Delivery:   delivery,
		Repository: scope.Repository.FullName,
		ReceivedAt: receivedAt.Format(time.RFC3339Nano),
		Payload:    body,
	}

	if len(body) > bodyCap {
		message.Payload, err = truncateGitHub(body)
	}

	return message, err
}

func truncateGitHub(body []byte) ([]byte, error) {
	var payload truncatedGitHub

	err := json.Unmarshal(body, &payload)
	if err != nil {
		return nil, fmt.Errorf("decode GitHub filter fields: %w", err)
	}

	payload.Truncated = true

	//nolint:errcheck,errchkjson // The retained fields have fixed JSON types.
	data, _ := json.Marshal(payload)

	return data, nil
}
