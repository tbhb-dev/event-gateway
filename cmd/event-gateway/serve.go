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
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	shutdownTimeout = 10 * time.Second
	readTimeout     = 15 * time.Second
	writeTimeout    = 30 * time.Second
)

var (
	errUsage      = errors.New("usage: event-gateway serve [flags]")
	errServeFlags = errors.New("invalid serve flags (see event-gateway serve -help)")
	errBodyCap    = errors.New("body cap must be positive and at most 5 MiB")
	errStoreLimit = errors.New("JetStream limits must be nonnegative")
)

type serveConfig struct {
	otlp       otlpConfig
	secret     *string
	secretFile *string
	listen     string
	storeDir   string
	bodyCap    int
	maxMemory  int64
	maxStore   int64
}

func (cfg *serveConfig) parse(out io.Writer, args []string) error {
	flags := flag.NewFlagSet("event-gateway serve", flag.ContinueOnError)
	// Flag parsing errors can quote argument values. Only print flag help.
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.listen, "listen", "127.0.0.1:8080", "HTTP listen address")
	flags.StringVar(&cfg.storeDir, "store-dir", "/data/nats", "JetStream data directory")
	flags.Func("github-secret", "GitHub webhook secret (overrides file and environment)", func(value string) error {
		cfg.secret = &value

		return nil
	})
	flags.Func("github-secret-file", "GitHub secret file (before GITHUB_WEBHOOK_SECRET)", func(value string) error {
		cfg.secretFile = &value

		return nil
	})
	flags.IntVar(&cfg.bodyCap, "body-cap", defaultBodyCap, "maximum retained raw body size in bytes")
	flags.Int64Var(
		&cfg.maxMemory,
		"jetstream-max-memory",
		0,
		"JetStream memory limit in bytes (0 uses the server default)",
	)
	flags.Int64Var(&cfg.maxStore, "jetstream-max-store", 0, "JetStream disk limit in bytes (0 uses the server default)")
	flags.StringVar(&cfg.otlp.Endpoint, "otlp-endpoint", "", "OTLP HTTP collector base URL")
	flags.StringVar(&cfg.otlp.CAFile, "otlp-ca-file", "", "PEM CA file for the collector")

	cfg.otlp.Headers = make(map[string]string)

	flags.Func("otlp-header", "OTLP collector header as name=value (repeatable)", func(header string) error {
		name, value, ok := strings.Cut(header, "=")
		if !ok || name == "" {
			return errServeFlags
		}

		cfg.otlp.Headers[name] = value

		return nil
	})

	err := flags.Parse(args)
	switch {
	case errors.Is(err, flag.ErrHelp):
		flags.SetOutput(out)
		flags.Usage()

		return flag.ErrHelp
	case err != nil:
		return errServeFlags
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

func (cfg *serveConfig) loadSecret() ([]byte, error) {
	switch {
	case cfg.secret != nil:
		return []byte(*cfg.secret), nil
	case cfg.secretFile != nil:
		secret, err := os.ReadFile(*cfg.secretFile)
		if err != nil {
			return nil, fmt.Errorf("read GitHub secret file: %w", err)
		}

		return bytes.TrimRight(secret, "\r\n"), nil
	default:
		return []byte(os.Getenv("GITHUB_WEBHOOK_SECRET")), nil
	}
}

func serve(ctx context.Context, out io.Writer, logHandler slog.Handler, args []string) error {
	var cfg serveConfig

	err := cfg.parse(out, args)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}

	if err != nil {
		return err
	}

	secret, err := cfg.loadSecret()
	if err != nil {
		return err
	}

	handler, err := newGitHubWebhook(secret, cfg.bodyCap)
	if err != nil {
		return err
	}

	options := storeOptions(cfg.storeDir)
	options.JetStreamMaxMemory = cfg.maxMemory
	options.JetStreamMaxStore = cfg.maxStore

	store, closeStore, err := openStore(ctx, options, retention)
	if err != nil {
		return err
	}

	defer closeStore()

	signals, closeSignals, err := openTelemetry(ctx, logHandler, cfg.otlp)
	if err != nil {
		return err
	}

	defer closeSignals()

	handler.store = store
	handler.telemetry = signals

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
