// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRunPrintsVersion(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer

	run(&out)

	if got, want := out.String(), "event-gateway dev\n"; got != want {
		t.Errorf("run() wrote %q, want %q", got, want)
	}
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
