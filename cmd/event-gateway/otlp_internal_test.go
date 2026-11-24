// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"bytes"
	"crypto/tls"
	"encoding/pem"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr/funcr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	collectorToken = "Bearer collector-fixture"
	collectorURL   = "https://collector.example"
	authorization  = "Authorization"
)

func TestOTLPExplicitConfig(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://[invalid")
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "X-Bogus=env-fixture")

	var paths sync.Map

	collector := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		checkEqual(t, request.Header.Get(authorization), collectorToken)
		checkEqual(t, request.Header.Get("X-Bogus"), "")

		paths.Store(request.URL.Path, true)
		response.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(collector.Close)

	caFile := filepath.Join(t.TempDir(), "collector.pem")
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Headers: nil, Bytes: collector.Certificate().Raw})
	checkError(t, os.WriteFile(caFile, cert, 0o600))

	_, _, err := openTelemetry(t.Context(), slog.DiscardHandler, otlpConfig{Endpoint: "", Headers: nil, CAFile: ""})
	checkError(t, err)

	signals, closeSignals, err := openTelemetry(t.Context(), slog.DiscardHandler, otlpConfig{
		Endpoint: collector.URL + "/otlp",
		Headers:  map[string]string{authorization: collectorToken},
		CAFile:   caFile,
	})
	checkError(t, err)

	hook := &githubWebhook{
		store: testStore(t, retention), telemetry: signals, secret: []byte(fixtureKey), bodyCap: defaultBodyCap,
	}
	expectStatus(t, hook.ServeHTTP, signedRequest(t.Context(), issuesEvent, "1", "{}"), http.StatusAccepted)
	conflict := signedRequest(t.Context(), issuesEvent, "1", `{"text":"raw-payload-fixture"}`)
	expectStatus(t, hook.ServeHTTP, conflict, http.StatusConflict)
	closeSignals()

	for _, path := range []string{"/otlp/v1/metrics", "/otlp/v1/traces", "/otlp/v1/logs"} {
		_, seen := paths.Load(path)
		checkEqual(t, seen, true)
	}
}

func TestCollectorTLS(t *testing.T) {
	t.Parallel()

	writer, err := newOTLPWriter(otlpConfig{Endpoint: collectorURL, Headers: nil, CAFile: ""})
	checkError(t, err)

	transport, ok := writer.client.Transport.(*http.Transport)
	checkEqual(t, ok, true)
	checkEqual(t, transport.TLSClientConfig.MinVersion, uint16(tls.VersionTLS12))
	checkEqual(t, transport.TLSClientConfig.RootCAs == nil, true)

	badCA := filepath.Join(t.TempDir(), "collector.pem")
	_, err = newOTLPWriter(otlpConfig{Endpoint: collectorURL, Headers: nil, CAFile: badCA})
	checkEqual(t, errors.Is(err, errCollectorCA), true)

	checkError(t, os.WriteFile(badCA, []byte("invalid"), 0o600))
	_, err = newOTLPWriter(otlpConfig{Endpoint: collectorURL, Headers: nil, CAFile: badCA})
	checkEqual(t, errors.Is(err, errCollectorCA), true)
}

func TestOTLPEndpointValidation(t *testing.T) {
	t.Parallel()

	for _, endpoint := range []string{
		"https://:4318", "http://[invalid", "https://user@collector.example",
		"ftp://collector.example", "http://192.0.2.1:4318", "http://collector.example",
		"https://host:4318/v1?tenant=x", "https://host:4318/#frag", "https://host:4318/?",
	} {
		cfg := otlpConfig{Endpoint: endpoint, Headers: nil, CAFile: ""}
		_, _, err := openTelemetry(t.Context(), slog.DiscardHandler, cfg)
		checkEqual(t, errors.Is(err, errOTLPEndpoint), true)
	}

	_, err := newOTLPWriter(otlpConfig{Endpoint: "http://localhost:4318", Headers: nil, CAFile: ""})
	checkError(t, err)
}

func TestOTLPRedirects(t *testing.T) {
	t.Parallel()

	for _, status := range []int{
		http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect,
	} {
		collector := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			if request.URL.Path == "/v1/traces" {
				http.Redirect(response, request, "/target", status)

				return
			}

			t.Errorf("redirect %d sent %s to %s", status, request.Method, request.URL.Path)
		}))
		t.Cleanup(collector.Close)

		writer, err := newOTLPWriter(otlpConfig{Endpoint: collector.URL, Headers: nil, CAFile: ""})
		checkError(t, err)
		checkEqual(t, writer.UploadTraces(t.Context(), nil), errOTLPExport)

		for _, target := range []string{"https" + strings.TrimPrefix(collector.URL, "http"), "http://other.example"} {
			next := httptest.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
			checkEqual(t, writer.client.CheckRedirect(next, nil), http.ErrUseLastResponse)
		}
	}
}

//nolint:paralleltest // The SDK's diagnostics are process-wide.
func TestOTLPFailureKeepsWebhookAccepted(t *testing.T) {
	collector := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		http.Error(response, collectorToken, http.StatusBadRequest)
	}))
	t.Cleanup(collector.Close)

	previous := otel.GetErrorHandler()

	t.Cleanup(func() { otel.SetErrorHandler(previous) })
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { t.Errorf("SDK exposed an error: %v", err) }))
	otel.SetLogger(funcr.New(func(prefix, args string) {
		t.Errorf("SDK exposed a diagnostic: %s %s", prefix, args)
	}, funcr.Options{})) //nolint:exhaustruct_v5 // The test uses default logger options.

	var logs bytes.Buffer

	signals, closeSignals, err := openTelemetry(t.Context(), slog.NewJSONHandler(&logs, nil), otlpConfig{
		Endpoint: collector.URL, Headers: map[string]string{authorization: collectorToken}, CAFile: "",
	})
	checkError(t, err)
	hook := &githubWebhook{
		store: testStore(t, retention), telemetry: signals, secret: []byte(fixtureKey), bodyCap: defaultBodyCap,
	}
	expectStatus(t, hook.ServeHTTP, signedRequest(t.Context(), issuesEvent, "1", "{}"), http.StatusAccepted)

	otel.Handle(errFixtureExport)
	closeSignals()
	checkEqual(t, strings.Contains(logs.String(), collectorToken), false)
	checkEqual(t, strings.Contains(logs.String(), "OTLP shutdown failed"), true)

	collector.Close()
	writer, err := newOTLPWriter(otlpConfig{Endpoint: collector.URL, Headers: nil, CAFile: ""})
	checkError(t, err)
	//nolint:exhaustruct_v5 // Simulate an unsupported SDK instrument.
	unusedMetric := &metricdata.ResourceMetrics{
		ScopeMetrics: []metricdata.ScopeMetrics{{Metrics: []metricdata.Metrics{{}}}},
	}
	checkEqual(t, (otlpMetrics{writer}).Export(t.Context(), unusedMetric), errOTLPExport)
	checkEqual(t, writer.send(t.Context(), "traces", otlpString("\xff")), errOTLPExport)

	writer.endpoint.Host = "invalid host"
	checkEqual(t, writer.UploadTraces(t.Context(), nil), errOTLPExport)
}

var errFixtureExport = errors.New("collector reported " + collectorToken)
