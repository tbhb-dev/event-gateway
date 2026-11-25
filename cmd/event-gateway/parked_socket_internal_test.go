// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

//go:build unix && !aix && (!solaris || illumos)

//nolint:exhaustruct_v5 // The fixtures use only their socket and target fields.
package main

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestParkedSocket(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	t.Chdir(dir)

	const path = "s"

	args := []string{parkedCommand, parkedListCommand, "-socket=" + path}
	notRunning := "event-gateway is not running (cannot reach " + path + ")"
	checkEqual(t, run(ctx, io.Discard, slog.DiscardHandler, args).Error(), notRunning)

	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	checkError(t, err)
	stale.SetUnlinkOnClose(false)
	checkError(t, stale.Close())
	checkEqual(t, run(ctx, io.Discard, slog.DiscardHandler, args).Error(), notRunning)

	targets := []runnerTarget{{Target: fakeTarget{name: "replay"}}}
	cfg := serveConfig{storeDir: dir, parkedPath: path}
	checkError(t, cfg.withParked(ctx, targets, func(parkedService) error {
		info, statErr := os.Stat(path)
		checkError(t, statErr)
		checkEqual(t, info.Mode().Perm(), os.FileMode(0o600))

		busy := cfg.withParked(ctx, targets, nil)
		checkEqual(t, errors.Is(busy, syscall.EADDRINUSE), true)

		return nil
	}))

	_, err = os.Lstat(path)
	checkEqual(t, errors.Is(err, os.ErrNotExist), true)
	checkError(t, os.WriteFile(path, nil, 0o600))
	_, err = listenParked(ctx, path)
	checkEqual(t, errors.Is(err, syscall.EADDRINUSE), true)

	cfg.storeDir = path
	checkEqual(t, errors.Is(cfg.withParked(ctx, targets, nil), syscall.ENOTDIR), true)

	closed, err := listenParked(ctx, filepath.Join(dir, "b"))
	checkError(t, err)
	_, err = secureParkedSocket(closed, filepath.Join(dir, "missing"))
	checkEqual(t, errors.Is(err, os.ErrNotExist), true)
	clearStaleSocket(ctx, filepath.Join(dir, "missing", "s"))
}
