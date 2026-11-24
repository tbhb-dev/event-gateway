// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	healthcheckCommand = "healthcheck"
	healthcheckTimeout = 4 * time.Second
)

var (
	errHealthcheckUsage = errors.New("usage: event-gateway healthcheck [-addr HOST:PORT]")
	errHealthcheck      = errors.New("healthcheck failed")
)

// healthcheck probes GET /healthz on the address and succeeds only on HTTP 200.
// Errors never include the address or the response body.
func healthcheck(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("event-gateway healthcheck", flag.ContinueOnError)
	// Flag parsing errors can quote argument values.
	flags.SetOutput(io.Discard)

	addr := flags.String("addr", "127.0.0.1:8080", "server address to probe")

	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return errHealthcheckUsage
	}

	ctx, cancel := context.WithTimeout(ctx, healthcheckTimeout)
	defer cancel()

	//nolint:exhaustruct_v5 // The probe URL needs only scheme, host and path.
	target := url.URL{Scheme: "http", Host: *addr, Path: "/healthz"}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return errHealthcheck
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errHealthcheck
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return errHealthcheck
	}

	return nil
}
