package app

import (
	"context"
	"fmt"
	"os"
	"runtime"

	"github.com/graphene-ci/agent/internal/agent"
	"github.com/graphene-ci/agent/internal/artifact"
	"github.com/graphene-ci/agent/internal/command"
	"github.com/graphene-ci/agent/internal/config"
	"github.com/graphene-ci/agent/internal/facts"
	"github.com/graphene-ci/agent/internal/secret"
	"github.com/graphene-ci/agent/internal/session"
	"github.com/graphene-ci/agent/internal/state"
	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
)

func Run(ctx context.Context, cfg config.Config, lookupEnv func(string) (string, bool)) error {
	if runtime.GOOS != "linux" {
		return fmt.Errorf("graphene-agent v1 supports Linux, got %s", runtime.GOOS)
	}
	token, err := secret.Load(cfg.Auth.TokenFile, lookupEnv)
	if err != nil {
		return err
	}
	store, err := state.Open(cfg.State.Path)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	installationID, err := store.InstallationID()
	if err != nil {
		return err
	}
	connection, err := session.Dial(cfg.Server, token)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	client := agentpb.NewAgentServiceClient(connection)
	outbox := session.NewOutbox()
	factReader := facts.New(facts.Config{
		Timeout: cfg.Runtime.ProbeTimeout, AllowSensitive: cfg.Facts.AllowSensitive, MaxItems: cfg.Facts.MaxItems,
	})
	runtimeAgent := agent.New(
		store,
		outbox,
		factReader,
		artifact.New(client, cfg.Artifact.ChunkBytes, cfg.Artifact.DefaultMode),
		agent.Config{
			Command: command.Config{
				Shell: cfg.Runtime.Shell, WorkingDirectory: cfg.Runtime.WorkingDirectory,
				DefaultTimeout: cfg.Runtime.DefaultTimeout, TerminateGrace: cfg.Runtime.ShutdownTimeout,
			},
			GlobalOutput:  cfg.Output.GlobalPendingBytes,
			OutputDrain:   cfg.Output.DrainTimeout,
			MaxConcurrent: cfg.Runtime.MaxConcurrent,
		},
	)
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("read hostname: %w", err)
	}
	controlSession := session.New(client, outbox, runtimeAgent, session.Config{
		Heartbeat:    cfg.Runtime.Heartbeat,
		ReconnectMin: cfg.Runtime.ReconnectMin,
		ReconnectMax: cfg.Runtime.ReconnectMax,
		Hello: &agentpb.Hello{
			InstallationId:        &agentpb.InstallationId{Value: installationID},
			ProtocolVersion:       "1",
			Hostname:              hostname,
			OperatingSystem:       runtime.GOOS,
			Architecture:          runtime.GOARCH,
			SupportedFactGroups:   factReader.SupportedGroups(),
			FactSchemaVersion:     facts.SchemaVersion,
			SensitiveFactsAllowed: factReader.SensitiveAllowed(),
			Capabilities: []agentpb.Capability{
				agentpb.Capability_CAPABILITY_COMMAND,
				agentpb.Capability_CAPABILITY_TERMINAL,
				agentpb.Capability_CAPABILITY_PUT_ARTIFACT,
				agentpb.Capability_CAPABILITY_COLLECT_ARTIFACT,
				agentpb.Capability_CAPABILITY_FACTS,
			},
		},
	})
	return controlSession.Run(ctx)
}
