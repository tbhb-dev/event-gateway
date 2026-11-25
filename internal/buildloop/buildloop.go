// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Package buildloop runs the commands of a build-loop step.
package buildloop

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

const (
	// OutputLimit is the most output, in bytes, a Runner captures from one command.
	OutputLimit = 1 << 20

	defaultTimeout = 10 * time.Minute
	waitDelay      = 5 * time.Second
)

// Command describes one command to run. Executable is matched exactly against
// the Runner's allow-list and is never run through a shell. A zero Timeout
// means ten minutes, and an empty Dir means the current directory.
type Command struct {
	Executable string
	Dir        string
	Args       []string
	Timeout    time.Duration
}

// Result is the outcome of a command that ran to completion. Output holds the
// standard output and standard error, interleaved and capped at OutputLimit.
type Result struct {
	Output    string
	Duration  time.Duration
	ExitCode  int
	Truncated bool
}

// TimeoutError reports a command that did not finish within its timeout.
type TimeoutError struct {
	Timeout time.Duration
}

// Error implements error.
func (e *TimeoutError) Error() string {
	return fmt.Sprintf("command exceeded its timeout of %s", e.Timeout)
}

// OutputCapError reports a command whose output went past OutputLimit. The
// Result returned with it holds the first OutputLimit bytes.
type OutputCapError struct {
	Limit int
}

// Error implements error.
func (e *OutputCapError) Error() string {
	return fmt.Sprintf("command output exceeded %d bytes", e.Limit)
}

// NotAllowedError reports an executable that is not on the allow-list.
type NotAllowedError struct {
	Executable string
}

// Error implements error.
func (e *NotAllowedError) Error() string {
	return fmt.Sprintf("command %q is not allowed", e.Executable)
}

// Runner runs commands whose executable is on a fixed allow-list.
type Runner struct {
	allowed map[string]struct{}
}

// NewRunner returns a Runner that runs only the named executables. A name must
// match Command.Executable exactly: the Runner does not clean paths, fold
// case, or take the base name.
func NewRunner(allowed ...string) *Runner {
	set := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		set[name] = struct{}{}
	}

	return &Runner{allowed: set}
}

// Run runs one command and waits for it. A command that exits with a nonzero
// status is not an error: the status is in Result.ExitCode. Run returns a
// *NotAllowedError, *TimeoutError, or *OutputCapError for those failures, and
// the Result of the last two holds the output captured so far.
func (r *Runner) Run(ctx context.Context, command Command) (Result, error) {
	if _, ok := r.allowed[command.Executable]; !ok {
		return Result{}, &NotAllowedError{Executable: command.Executable}
	}

	timeout := cmp.Or(command.Timeout, defaultTimeout)

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var out capture

	//nolint:gosec // The executable is matched against the allow-list above.
	cmd := exec.CommandContext(ctx, command.Executable, command.Args...)
	cmd.Dir = command.Dir
	cmd.Stdout = &out
	cmd.Stderr = &out
	cmd.WaitDelay = waitDelay

	start := time.Now()
	err := cmd.Run()
	result := Result{
		Output:    out.buf.String(),
		Duration:  time.Since(start),
		ExitCode:  cmd.ProcessState.ExitCode(),
		Truncated: out.truncated,
	}

	var exitErr *exec.ExitError

	switch {
	case err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded):
		return result, &TimeoutError{Timeout: timeout}
	case err != nil && ctx.Err() != nil:
		return result, fmt.Errorf("run %q: %w", command.Executable, ctx.Err())
	case err != nil && !errors.As(err, &exitErr):
		return result, fmt.Errorf("run %q: %w", command.Executable, err)
	case out.truncated:
		return result, &OutputCapError{Limit: OutputLimit}
	default:
		return result, nil
	}
}

// capture keeps the first OutputLimit bytes written to it and discards the
// rest, so the command never blocks on a full pipe.
type capture struct {
	buf       bytes.Buffer
	truncated bool
}

// Write implements io.Writer.
func (c *capture) Write(data []byte) (int, error) {
	size := len(data)

	if room := OutputLimit - c.buf.Len(); size > room {
		c.truncated = true
		data = data[:room]
	}

	c.buf.Write(data)

	return size, nil
}
