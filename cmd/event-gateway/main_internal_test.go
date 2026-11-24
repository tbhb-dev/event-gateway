// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	storeDirFlag   = "-store-dir="
	testReadListen = "-read-listen=127.0.0.1:0"
)

func TestRunPrintsVersion(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer

	checkError(t, run(t.Context(), &out, slog.DiscardHandler, nil))
	checkEqual(t, out.String(), "event-gateway dev\n")

	reader, writer := io.Pipe()
	checkError(t, reader.Close())
	t.Cleanup(func() { checkError(t, writer.Close()) })

	err := run(t.Context(), writer, slog.DiscardHandler, nil)
	checkEqual(t, errors.Is(err, io.ErrClosedPipe), true)
}

func TestServeConfigErrors(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		err  error
		name string
		args []string
	}{
		{errUsage, "command", []string{"unknown"}},
		{nil, "help", []string{serveCommand, "-help"}},
		{errUsage, "extra argument", []string{serveCommand, "extra"}},
		{errBodyCap, "empty cap", []string{serveCommand, "-body-cap=0"}},
		{errBodyCap, "large cap", []string{serveCommand, "-body-cap=6000000"}},
		{errStoreLimit, "negative memory", []string{serveCommand, "-jetstream-max-memory=-1"}},
		{errStoreLimit, "negative disk", []string{serveCommand, "-jetstream-max-store=-1"}},
		{errReadFlags, "missing read token file flag", []string{serveCommand, testReadListen}},
		{errReadFlags, "empty read address", []string{serveCommand, "-read-listen=", "-read-token-file=tokens"}},
		{os.ErrNotExist, "missing read token file", []string{serveCommand, testReadListen, "-read-token-file=missing"}},
		{errServeFlags, "header without separator", []string{serveCommand, "-otlp-header=Authorization"}},
		{errServeFlags, "empty header name", []string{serveCommand, "-otlp-header==value"}},
		{errStoreDir, "store directory", []string{serveCommand, storeDirFlag}},
		{errOTLPEndpoint, "collector endpoint", []string{serveCommand, "-otlp-endpoint=invalid"}},
		{errCollectorCA, "collector CA", []string{serveCommand, "-otlp-endpoint=https://localhost", "-otlp-ca-file=missing"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			args := append(
				[]string{test.args[0], storeDirFlag + t.TempDir(), "-github-secret=" + fixtureKey},
				test.args[1:]...,
			)
			err := run(t.Context(), io.Discard, slog.DiscardHandler, args)
			checkEqual(t, errors.Is(err, test.err), true)
		})
	}
}

func TestServeRejectsInvalidSecret(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", "")
	checkError(t, os.Unsetenv("GITHUB_WEBHOOK_SECRET"))
	missingErr := run(t.Context(), io.Discard, slog.DiscardHandler, []string{serveCommand, storeDirFlag})
	checkEqual(t, errors.Is(missingErr, errSecret), true)

	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	blank := filepath.Join(dir, "blank")

	checkError(t, os.WriteFile(empty, nil, 0o600))
	checkError(t, os.WriteFile(blank, []byte(" \t\r\n"), 0o600))

	for _, test := range []struct {
		err  error
		name string
		env  string
		args []string
	}{
		{errSecret, "empty environment", "", nil},
		{errSecret, "blank environment", " \t", nil},
		{errSecret, "empty flag", fixtureKey, []string{"-github-secret="}},
		{errSecret, "blank flag", fixtureKey, []string{"-github-secret= \t"}},
		{errSecret, "empty file", fixtureKey, []string{"-github-secret-file=" + empty}},
		{errSecret, "blank file", fixtureKey, []string{"-github-secret-file=" + blank}},
		{os.ErrNotExist, "missing file", fixtureKey, []string{"-github-secret-file=" + filepath.Join(dir, "missing")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("GITHUB_WEBHOOK_SECRET", test.env)

			args := append([]string{serveCommand, storeDirFlag, "-listen=127.0.0.1:-1"}, test.args...)
			err := run(t.Context(), io.Discard, slog.DiscardHandler, args)
			checkEqual(t, errors.Is(err, test.err), true)
		})
	}
}

func TestGitHubWebhookRequiresSecret(t *testing.T) {
	t.Parallel()

	for _, secret := range [][]byte{nil, {}, []byte(" \t")} {
		hook, err := newGitHubWebhook(secret, defaultBodyCap)
		checkEqual(t, hook, nil)
		checkEqual(t, errors.Is(err, errSecret), true)
	}
}

func TestServeInvalidListenAddress(t *testing.T) {
	t.Parallel()

	args := []string{serveCommand, "-listen=127.0.0.1:-1", storeDirFlag + t.TempDir(), "-github-secret=" + fixtureKey}

	err := run(t.Context(), io.Discard, slog.DiscardHandler, args)
	if err == nil {
		t.Fatal("serve accepted an invalid listen address")
	}
}

func TestServeValidSecretAndContextShutdown(t *testing.T) {
	var exports atomic.Int32

	var sentHeader atomic.Bool

	collector := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		exports.Add(1)

		if request.Header.Get("X-Collector-Test") == "test=value" {
			sentHeader.Store(true)
		}

		response.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(collector.Close)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "X-Collector-Test=test=value")

	exerciseServe(t, nil)
	checkEqual(t, exports.Load(), 0)

	exerciseServe(t, []string{"-otlp-endpoint=" + collector.URL, "-otlp-header=X-Collector-Test=test=value"})
	checkEqual(t, exports.Load() > 0, true)
	checkEqual(t, sentHeader.Load(), true)
}

func exerciseServe(t *testing.T, collectorFlags []string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
	checkError(t, err)

	address := listener.Addr().String()
	checkError(t, listener.Close())

	dir := t.TempDir()
	file := filepath.Join(dir, "secret")
	checkError(t, os.WriteFile(file, []byte(fixtureKey+"\r\n"), 0o600))

	tokens := filepath.Join(dir, "tokens")
	writeReadTokens(t, tokens, "reader", readFixture)

	args := append(
		[]string{
			serveCommand, "-listen=" + address, storeDirFlag + dir, "-github-secret-file=" + file,
			testReadListen, "-read-token-file=" + tokens, "-grafana-secret-file=" + file,
		},
		collectorFlags...,
	)

	var logs bytes.Buffer

	done := make(chan error, 1)
	go func() {
		done <- run(ctx, io.Discard, slog.NewJSONHandler(&logs, nil), args)
	}()

	var client http.Client

	t.Cleanup(client.CloseIdleConnections)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+healthPath, nil)
	checkError(t, err)

	for {
		response, requestErr := client.Do(req)
		if requestErr == nil {
			checkError(t, response.Body.Close())
			checkEqual(t, response.StatusCode, http.StatusOK)

			break
		}

		select {
		case serveErr := <-done:
			t.Fatalf("gateway stopped before serving /healthz: %v", serveErr)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}

	const body = `{"repository":{"full_name":"example/repository"},"message":"private test payload"}`

	webhook := signedRequest(ctx, "push", "serve-test", body)
	webhook.RequestURI = ""
	webhook.URL.Scheme = "http"
	webhook.URL.Host = address
	webhook.Host = address
	response, err := client.Do(webhook)
	checkError(t, err)
	checkError(t, response.Body.Close())
	checkEqual(t, response.StatusCode, http.StatusAccepted)

	cancel()

	select {
	case serveErr := <-done:
		checkError(t, serveErr)
	case <-time.After(30 * time.Second):
		t.Fatal("gateway did not shut down after context cancellation")
	}

	checkEqual(t, strings.Contains(logs.String(), fixtureKey), false)
	checkEqual(t, strings.Contains(logs.String(), "private test payload"), false)
}

func TestExitCode(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	checkEqual(t, exitCode(t.Context(), logger, nil), 0)
	checkEqual(t, exitCode(t.Context(), logger, errUsage), 1)
}

func TestRoutes(t *testing.T) {
	t.Parallel()

	var hook githubWebhook

	for _, test := range []struct {
		method string
		path   string
		status int
	}{
		{http.MethodGet, healthPath, http.StatusOK},
		{http.MethodGet, webhookPath, http.StatusNotFound},
		{http.MethodPost, healthPath, http.StatusNotFound},
		{http.MethodPost, "/" + webhookPath, http.StatusNotFound},
	} {
		req := httptest.NewRequestWithContext(t.Context(), test.method, test.path, nil)
		expectStatus(t, hook.ServeHTTP, req, test.status)
	}
}
