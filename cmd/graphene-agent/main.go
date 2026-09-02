// Command graphene-agent hosts user code on one Linux machine: it
// connects outbound to the graphene server, reports machine facts, and
// runs per-(machine × run) worker containers. It listens on no ports and
// never speaks Temporal — the hosted user code does.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/graphene-ci/agent/internal/config"
	"github.com/graphene-ci/agent/internal/runtime/execproc"
	"github.com/graphene-ci/agent/internal/runtime/runcrt"
	"github.com/graphene-ci/agent/internal/session"
	"github.com/graphene-ci/agent/pkg/host"
)

var version = "dev"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println("graphene-agent", version)
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "graphene-agent:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.FromEnv()
	if err != nil {
		// No agent id yet — stderr only, no obs shipping.
		slog.New(slog.NewTextHandler(os.Stderr, nil)).Error("config", "error", err)
		return err
	}
	// The agent's own log tees to stderr (journald) AND to obs as
	// dimension-3 logs of agent/<id>, so it is read with `graphenectl logs
	// agent <id>` — not ssh + journalctl. The ship gets the live session
	// connection below; records before/between connections wait in its ring.
	ship := session.NewObsShip(string(cfg.AgentId))
	log := slog.New(session.NewObsHandler(slog.NewTextHandler(os.Stderr, nil), ship))
	store, err := session.OpenStore(cfg.DataDir)
	if err != nil {
		return err
	}
	var rt host.Runtime
	switch cfg.Runtime {
	case "runc":
		rt = runcrt.New(cfg.DataDir, runcrt.Options{Registry: cfg.Registry, Token: cfg.Token, Insecure: cfg.Insecure})
	case "exec":
		rt = execproc.New(cfg.DataDir)
	default:
		return fmt.Errorf("unknown runtime %q (%s)", cfg.Runtime, config.EnvRuntime)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("starting", "version", version, "machine", cfg.AgentId, "runtime", cfg.Runtime)
	return session.New(cfg, rt, store, version, log, ship).Run(ctx)
}
