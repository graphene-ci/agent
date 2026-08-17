package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/graphene-ci/agent/internal/app"
	"github.com/graphene-ci/agent/internal/config"
	"github.com/graphene-ci/agent/internal/version"
	"github.com/spf13/pflag"
)

func main() {
	os.Exit(run())
}

func run() int {
	parsed, err := config.Parse(os.Args[1:], os.LookupEnv, os.Environ, os.Stderr)
	if errors.Is(err, pflag.ErrHelp) {
		return 0
	}
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "configuration: %v\n", err)
		return 2
	}
	if parsed.ShowVersion {
		if _, err := fmt.Fprintln(os.Stdout, version.Value()); err != nil {
			return 1
		}
		return 0
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, parsed.Config, os.LookupEnv); err != nil {
		logger.Error("agent stopped", "error", err)
		return 1
	}
	return 0
}
