// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Command event-gateway receives and stores GitHub webhooks.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

const (
	version      = "dev"
	serveCommand = "serve"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Stdout, os.Args[1:])

	stop()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	os.Exit(exitCode(ctx, logger, err))
}

func exitCode(ctx context.Context, logger *slog.Logger, err error) int {
	if err != nil {
		logger.ErrorContext(ctx, "event gateway stopped", "error", err)

		return 1
	}

	return 0
}

func run(ctx context.Context, out io.Writer, args []string) error {
	if len(args) == 0 {
		_, err := fmt.Fprintf(out, "event-gateway %s\n", version)
		if err != nil {
			return fmt.Errorf("write version: %w", err)
		}

		return nil
	}

	if args[0] != serveCommand {
		return errUsage
	}

	return serve(ctx, out, args[1:])
}
