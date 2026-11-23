// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestRunPrintsVersion(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer

	checkError(t, run(t.Context(), &out, nil))
	checkEqual(t, out.String(), "event-gateway dev\n")

	reader, writer := io.Pipe()
	checkError(t, reader.Close())
	t.Cleanup(func() { checkError(t, writer.Close()) })

	err := run(t.Context(), writer, nil)
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
		{os.ErrNotExist, "missing file", []string{serveCommand}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := run(t.Context(), io.Discard, test.args)
			checkEqual(t, errors.Is(err, test.err), true)
		})
	}

	var cfg serveConfig

	err := cfg.parse(io.Discard, []string{"-help"})
	checkEqual(t, errors.Is(err, flag.ErrHelp), true)

	err = run(t.Context(), io.Discard, []string{serveCommand, "-unknown"})
	if err == nil {
		t.Fatal("serve accepted an unknown flag")
	}
}

func TestSecretFile(t *testing.T) {
	t.Parallel()

	file := filepath.Join(t.TempDir(), "secret")
	checkError(t, os.WriteFile(file, []byte("\r\n"), 0o600))
	_, err := readSecret(file)
	checkEqual(t, errors.Is(err, errSecret), true)

	checkError(t, os.WriteFile(file, []byte(fixtureKey+"\r\n"), 0o600))
	secret, err := readSecret(file)
	checkError(t, err)
	checkEqual(t, string(secret), fixtureKey)
}

func TestServeGracefulStop(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	httpServer := &http.Server{Addr: "127.0.0.1:0"} //nolint:exhaustruct_v5,gosec // No requests are served.
	checkError(t, runHTTP(ctx, httpServer))
}

func TestServeInvalidListenAddress(t *testing.T) {
	t.Parallel()

	args := serveArgs(t, "127.0.0.1:-1")

	err := run(t.Context(), io.Discard, args)
	if err == nil {
		t.Fatal("serve accepted an invalid listen address")
	}
}

func serveArgs(t *testing.T, listen string) []string {
	t.Helper()

	dir := t.TempDir()
	file := filepath.Join(dir, "secret")
	checkError(t, os.WriteFile(file, []byte(fixtureKey), 0o600))

	return []string{serveCommand, "-listen", listen, "-store-dir", dir, "-github-secret-file", file}
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

func TestExitCode(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.DiscardHandler)
	checkEqual(t, exitCode(t.Context(), logger, nil), 0)
	checkEqual(t, exitCode(t.Context(), logger, errUsage), 1)
}
