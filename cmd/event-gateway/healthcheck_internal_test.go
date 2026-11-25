// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthcheck(t *testing.T) {
	t.Parallel()

	healthy := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		checkEqual(t, r.URL.Path, "/healthz")
		checkEqual(t, r.Method, http.MethodGet)
	}))
	t.Cleanup(healthy.Close)

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "secret-body", http.StatusServiceUnavailable)
	}))
	t.Cleanup(bad.Close)

	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(target.Close)

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	t.Cleanup(redirect.Close)

	gone := httptest.NewServer(http.NotFoundHandler())
	closed := strings.TrimPrefix(gone.URL, "http://")
	gone.Close()

	okAddr := strings.TrimPrefix(healthy.URL, "http://")
	badAddr := strings.TrimPrefix(bad.URL, "http://")

	for _, test := range []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{"success", []string{"-addr", okAddr}, false},
		{"non-200", []string{"-addr=" + badAddr}, true},
		{"redirect", []string{"-addr=" + strings.TrimPrefix(redirect.URL, "http://")}, true},
		{"empty address", []string{"-addr="}, true},
		{"path", []string{"-addr=127.0.0.1:80/healthz"}, true},
		{"host path", []string{"-addr=127.0.0.1/x:80"}, true},
		{"query", []string{"-addr=127.0.0.1:80?x=1"}, true},
		{"fragment", []string{"-addr=127.0.0.1:80#x"}, true},
		{"whitespace", []string{"-addr=127.0.0.1 :80"}, true},
		{"refused", []string{"-addr=" + closed}, true},
		{"bad address", []string{"-addr=bad host"}, true},
		{"empty host", []string{"-addr=:8080"}, true},
		{"help", []string{"-help"}, true},
		{"unknown flag", []string{"-nope"}, true},
		{"extra args", []string{"-addr=" + okAddr, "extra"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			runErr := run(
				t.Context(),
				&strings.Builder{},
				slog.DiscardHandler,
				append([]string{healthcheckCommand}, test.args...),
			)
			checkEqual(t, runErr != nil, test.wantErr)

			if runErr != nil {
				for _, leak := range []string{okAddr, badAddr, closed, "secret-body"} {
					checkEqual(t, strings.Contains(runErr.Error(), leak), false)
				}
			}
		})
	}
}
