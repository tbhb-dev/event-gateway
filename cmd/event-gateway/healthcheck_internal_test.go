// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthcheck(t *testing.T) {
	t.Parallel()

	okServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" || r.Method != http.MethodGet {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(okServer.Close)

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		//nolint:errcheck,gosec // The client never reads a failed probe body.
		w.Write([]byte("secret-body"))
	}))
	t.Cleanup(bad.Close)

	var listenConfig net.ListenConfig

	listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	checkError(t, err)

	closed := listener.Addr().String()
	checkError(t, listener.Close())

	okAddr := strings.TrimPrefix(okServer.URL, "http://")
	badAddr := strings.TrimPrefix(bad.URL, "http://")

	for _, test := range []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{"success", []string{"-addr", okAddr}, false},
		{"non-200", []string{"-addr=" + badAddr}, true},
		{"refused", []string{"-addr=" + closed}, true},
		{"bad address", []string{"-addr=bad host"}, true},
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
