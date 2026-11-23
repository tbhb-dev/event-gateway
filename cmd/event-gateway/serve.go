// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	retention       = 7 * 24 * time.Hour
	defaultBodyCap  = 5 << 20
	shutdownTimeout = 10 * time.Second
	readTimeout     = 15 * time.Second
	writeTimeout    = 30 * time.Second
)

var (
	errUsage      = errors.New("usage: event-gateway serve [flags]")
	errSecret     = errors.New("GitHub secret file is empty")
	errBodyCap    = errors.New("body cap must be positive and at most 5 MiB")
	errStoreLimit = errors.New("JetStream limits must be nonnegative")
)

type serveConfig struct {
	listen     string
	storeDir   string
	secretFile string
	bodyCap    int
	maxMemory  int64
	maxStore   int64
}

func (cfg *serveConfig) parse(out io.Writer, args []string) error {
	flags := flag.NewFlagSet("event-gateway serve", flag.ContinueOnError)
	flags.SetOutput(out)
	flags.StringVar(&cfg.listen, "listen", "127.0.0.1:8080", "HTTP listen address")
	flags.StringVar(&cfg.storeDir, "store-dir", "/data/nats", "JetStream data directory")
	flags.StringVar(&cfg.secretFile, "github-secret-file", "", "file containing the GitHub webhook secret")
	flags.IntVar(&cfg.bodyCap, "body-cap", defaultBodyCap, "maximum retained raw body size in bytes")
	flags.Int64Var(
		&cfg.maxMemory,
		"jetstream-max-memory",
		0,
		"JetStream memory limit in bytes (0 uses the server default)",
	)
	flags.Int64Var(&cfg.maxStore, "jetstream-max-store", 0, "JetStream disk limit in bytes (0 uses the server default)")

	err := flags.Parse(args)
	if err != nil {
		return fmt.Errorf("parse serve flags: %w", err)
	}

	switch {
	case flags.NArg() != 0:
		return errUsage
	case cfg.bodyCap <= 0, cfg.bodyCap > defaultBodyCap:
		return errBodyCap
	case cfg.maxMemory < 0, cfg.maxStore < 0:
		return errStoreLimit
	default:
		return nil
	}
}

func readSecret(path string) ([]byte, error) {
	secret, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read GitHub secret file: %w", err)
	}

	secret = bytes.TrimRight(secret, "\r\n")
	if len(secret) == 0 {
		return nil, errSecret
	}

	return secret, nil
}

func serve(ctx context.Context, out io.Writer, args []string) error {
	var cfg serveConfig

	err := cfg.parse(out, args)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}

	if err != nil {
		return err
	}

	secret, err := readSecret(cfg.secretFile)
	if err != nil {
		return err
	}

	signals, err := openTelemetry(ctx, out)
	if err != nil {
		return err
	}

	defer signals.close(ctx)

	store, closeStore, err := openStore(ctx, storeOptions(cfg), retention)
	if err != nil {
		return err
	}

	defer closeStore()

	handler := &githubWebhook{
		secret:    secret,
		bodyCap:   cfg.bodyCap,
		store:     store,
		telemetry: signals,
	}

	//nolint:exhaustruct_v5 // The gateway sets the address, handler, and timeouts it needs.
	httpServer := &http.Server{
		Addr:                         cfg.listen,
		Handler:                      handler,
		DisableGeneralOptionsHandler: true,
		ReadHeaderTimeout:            readTimeout,
		ReadTimeout:                  readTimeout,
		WriteTimeout:                 writeTimeout,
	}

	return runHTTP(ctx, httpServer)
}

func runHTTP(ctx context.Context, httpServer *http.Server) error {
	done := make(chan error, 1)
	stop := context.AfterFunc(ctx, func() {
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()

		done <- errors.Join(httpServer.Shutdown(shutdown), httpServer.Close())
	})

	err := httpServer.ListenAndServe()
	if stop() {
		return fmt.Errorf("serve HTTP: %w", err)
	}

	return <-done
}
