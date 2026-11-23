// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"bytes"
	"context"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
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
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)

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

func TestServeOTLPConfigError(t *testing.T) {
	collector := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(collector.Close)

	cert := pem.EncodeToMemory(&pem.Block{
		Type:    "CERTIFICATE",
		Headers: nil,
		Bytes:   collector.Certificate().Raw,
	})
	file := filepath.Join(t.TempDir(), "certificate")
	checkError(t, os.WriteFile(file, cert, 0o600))
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", file)

	err := run(t.Context(), io.Discard, serveArgs(t, "127.0.0.1:0"))
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
