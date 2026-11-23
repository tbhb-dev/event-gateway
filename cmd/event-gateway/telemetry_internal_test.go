// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"bytes"
	"context"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
)

func TestOTLP(t *testing.T) {
	requests := make(chan string, 16)
	collector := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
		}

		if bytes.Contains(body, []byte("raw-payload-fixture")) {
			t.Error("collector received raw payload")
		}

		requests <- request.URL.Path

		response.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(collector.Close)

	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", collector.URL+"/v1/traces")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", collector.URL+"/v1/metrics")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", collector.URL+"/v1/logs")

	var logs bytes.Buffer

	store := testStore(t, retention)
	signals, err := openTelemetry(t.Context(), &logs)
	checkError(t, err)

	hook := &githubWebhook{store: store, telemetry: signals, secret: []byte(fixtureKey), bodyCap: defaultBodyCap}
	expectStatus(t, hook.ServeHTTP, signedRequest(t.Context(), issuesEvent, "1", "{}"), http.StatusAccepted)
	expectStatus(
		t,
		hook.ServeHTTP,
		signedRequest(t.Context(), issuesEvent, "1", `{"text":"raw-payload-fixture"}`),
		http.StatusConflict,
	)
	signals.close(t.Context())
	signals.close(t.Context())

	if bytes.Contains(logs.Bytes(), []byte("shutdown failed")) {
		t.Fatal("second close reported a failure")
	}

	if bytes.Contains(logs.Bytes(), []byte("raw-payload-fixture")) {
		t.Fatal("logger received raw payload")
	}

	close(requests)

	got := make(map[string]bool)
	for path := range requests {
		got[path] = true
	}

	for _, path := range []string{"/v1/traces", "/v1/metrics", "/v1/logs"} {
		if !got[path] {
			t.Errorf("collector did not receive %s", path)
		}
	}
}

var errExport = errors.New("export failed with Authorization=Bearer " + syntheticLeak)

const (
	otlpEndpoint   = "OTEL_EXPORTER_OTLP_ENDPOINT"
	otlpHeaders    = "OTEL_EXPORTER_OTLP_HEADERS"
	syntheticLeak  = "synthetic-credential"
	collectorHTTPS = "https://collector.example"
)

func TestOTLPEnvRejected(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"malformed endpoint": {otlpEndpoint: "http://[::1"},
		"wrong scheme":       {otlpEndpoint: "ftp://collector.example"},
		"no host":            {otlpEndpoint: "https://"},
		"remote address":     {otlpEndpoint: "http://192.0.2.1:4318"},
		"header without key": {otlpEndpoint: collectorHTTPS, otlpHeaders: "=" + syntheticLeak},
		"key with space":     {otlpEndpoint: collectorHTTPS, otlpHeaders: "Authorization: Bearer " + syntheticLeak + "=="},
		"remote plaintext":   {otlpEndpoint: "http://collector.example:4318"},
		"header syntax":      {otlpEndpoint: collectorHTTPS, otlpHeaders: "Authorization: Bearer " + syntheticLeak},
		"signal header syntax": {
			otlpEndpoint:                        collectorHTTPS,
			"OTEL_EXPORTER_OTLP_TRACES_HEADERS": "Authorization: Bearer " + syntheticLeak,
		},
	} {
		t.Run(name, func(t *testing.T) {
			for key, value := range env {
				t.Setenv(key, value)
			}

			var logs bytes.Buffer

			_, err := openTelemetry(t.Context(), &logs)
			if err == nil {
				t.Fatal("OTLP accepted unsafe settings")
			}

			if strings.Contains(err.Error(), syntheticLeak) || strings.Contains(logs.String(), syntheticLeak) {
				t.Fatal("credential reached the error or the log")
			}
		})
	}
}

func TestOTLPErrorHandler(t *testing.T) {
	t.Setenv(otlpEndpoint, "http://localhost:4318")
	t.Setenv(otlpHeaders, "Authorization=Bearer "+syntheticLeak)

	var logs bytes.Buffer

	_, err := openTelemetry(t.Context(), &logs)
	checkError(t, err)
	otel.Handle(errExport)

	if !strings.Contains(logs.String(), "OTLP export failed") || strings.Contains(logs.String(), syntheticLeak) {
		t.Fatalf("SDK error was not reduced to a fixed message: %s", logs.String())
	}
}

func TestOTLPConfigError(t *testing.T) {
	collector := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(collector.Close)

	cert := pem.EncodeToMemory(&pem.Block{
		Type:    "CERTIFICATE",
		Headers: nil,
		Bytes:   collector.Certificate().Raw,
	})
	file := filepath.Join(t.TempDir(), "certificate")
	checkError(t, os.WriteFile(file, cert, 0o600))
	t.Setenv(otlpEndpoint, "http://127.0.0.1:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", file)

	_, err := openTelemetry(t.Context(), io.Discard)
	if err == nil {
		t.Fatal("OTLP accepted plaintext with a TLS certificate")
	}
}

func TestTelemetryShutdownError(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer

	signals := telemetry{ //nolint:exhaustruct_v5 // Shutdown only needs the logger and shutdown function.
		logger:   slog.New(slog.NewJSONHandler(&logs, nil)),
		shutdown: func(context.Context) error { return context.DeadlineExceeded },
	}
	signals.close(t.Context())

	if !bytes.Contains(logs.Bytes(), []byte("OTLP shutdown failed")) {
		t.Fatal("shutdown error was not logged")
	}
}
