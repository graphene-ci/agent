package testserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	runtimeagent "github.com/graphene-ci/agent/internal/agent"
	"github.com/graphene-ci/agent/internal/artifact"
	"github.com/graphene-ci/agent/internal/command"
	"github.com/graphene-ci/agent/internal/facts"
	"github.com/graphene-ci/agent/internal/session"
	"github.com/graphene-ci/agent/internal/state"
	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/grpc"
)

func TestAgentEndToEndOverBufconn(t *testing.T) {
	t.Parallel()
	service, connection, stop := testTransport(t, grpc.WithPerRPCCredentials(testBearerCredentials{token: testToken}))
	defer stop()
	client := agentpb.NewAgentServiceClient(connection)
	workingDirectory := t.TempDir()
	store, err := state.Open(filepath.Join(workingDirectory, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	outbox := session.NewOutbox()
	factReader := facts.New(facts.Config{Timeout: 5 * time.Second, MaxItems: 128})
	agent := runtimeagent.New(
		store,
		outbox,
		factReader,
		artifact.New(client, 4, 0o600),
		runtimeagent.Config{
			Command: command.Config{
				Shell: "/bin/sh", WorkingDirectory: workingDirectory,
				DefaultTimeout: time.Minute, TerminateGrace: 100 * time.Millisecond,
			},
			GlobalOutput: 1 << 20, OutputDrain: time.Second, MaxConcurrent: 4,
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	control := session.New(client, outbox, agent, session.Config{
		Heartbeat: time.Second, ReconnectMin: time.Millisecond, ReconnectMax: 10 * time.Millisecond,
		Hello: &agentpb.Hello{
			InstallationId: &agentpb.InstallationId{Value: "bufconn-installation"}, ProtocolVersion: "1",
			Capabilities: []agentpb.Capability{
				agentpb.Capability_CAPABILITY_COMMAND,
				agentpb.Capability_CAPABILITY_TERMINAL,
				agentpb.Capability_CAPABILITY_PUT_ARTIFACT,
				agentpb.Capability_CAPABILITY_COLLECT_ARTIFACT,
				agentpb.Capability_CAPABILITY_FACTS,
			},
			SupportedFactGroups: factReader.SupportedGroups(), FactSchemaVersion: facts.SchemaVersion,
		},
	})
	go func() { runResult <- control.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-runResult:
			if err != nil {
				t.Errorf("control session: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("control session did not stop")
		}
	}()

	hello := waitMatchingEvent(t, service.Events(), func(event *agentpb.ConnectRequest) bool { return event.GetHello() != nil })
	if hello.GetHello().GetInstallationId().GetValue() != "bufconn-installation" {
		t.Fatalf("hello = %#v", hello)
	}

	commandID := "e2e-command"
	if err := service.Submit(ctx, &agentpb.ConnectResponse{
		Id:          &agentpb.InstructionId{Value: commandID},
		Instruction: &agentpb.ConnectResponse_RunCommand{RunCommand: &agentpb.RunCommand{Command: "printf end-to-end"}},
	}); err != nil {
		t.Fatal(err)
	}
	var commandOutput []byte
	for {
		event := waitMatchingEvent(t, service.Events(), func(event *agentpb.ConnectRequest) bool {
			return event.GetInstructionId().GetValue() == commandID
		})
		if output := event.GetCommandOutput(); output != nil {
			commandOutput = append(commandOutput, output.GetData()...)
		}
		if exited := event.GetCommandExited(); exited != nil {
			if exited.GetExitCode() != 0 {
				t.Fatalf("command exited = %#v", exited)
			}
			break
		}
	}
	if string(commandOutput) != "end-to-end" {
		t.Fatalf("command output = %q", commandOutput)
	}

	factsID := "e2e-facts"
	if err := service.Submit(ctx, &agentpb.ConnectResponse{
		Id: &agentpb.InstructionId{Value: factsID},
		Instruction: &agentpb.ConnectResponse_ReadFacts{ReadFacts: &agentpb.ReadFacts{Groups: []agentpb.FactGroup{
			agentpb.FactGroup_FACT_GROUP_MEMORY,
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	factsEvent := waitMatchingEvent(t, service.Events(), func(event *agentpb.ConnectRequest) bool {
		return event.GetInstructionId().GetValue() == factsID && event.GetFactsRead() != nil
	})
	if results := factsEvent.GetFactsRead().GetResults(); len(results) != 1 || results[0].GetMemory() == nil {
		t.Fatalf("facts = %#v", factsEvent.GetFactsRead())
	}

	artifactSource := filepath.Join(workingDirectory, "server-source")
	artifactContent := []byte("artifact through bufconn")
	if err := os.WriteFile(artifactSource, artifactContent, 0o600); err != nil {
		t.Fatal(err)
	}
	metadata, err := service.AddArtifact(artifactSource)
	if err != nil {
		t.Fatal(err)
	}
	putID := "e2e-put"
	agentDestination := filepath.Join(workingDirectory, "agent-destination")
	if err := service.Submit(ctx, &agentpb.ConnectResponse{
		Id: &agentpb.InstructionId{Value: putID},
		Instruction: &agentpb.ConnectResponse_PutArtifact{PutArtifact: &agentpb.PutArtifact{
			ArtifactId: metadata.GetArtifactId(), Path: agentDestination, Overwrite: true,
			Size: metadata.GetSize(), Sha256: metadata.GetSha256(),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	placed := waitMatchingEvent(t, service.Events(), func(event *agentpb.ConnectRequest) bool {
		return event.GetInstructionId().GetValue() == putID && event.GetArtifactPlaced() != nil
	})
	if placed.GetArtifactPlaced().GetPath() != agentDestination {
		t.Fatalf("placed = %#v", placed.GetArtifactPlaced())
	}
	downloaded, err := os.ReadFile(agentDestination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(downloaded, artifactContent) {
		t.Fatalf("downloaded = %q", downloaded)
	}

	collectID := "e2e-collect"
	collectedArtifactID := "e2e-collected-artifact"
	if err := service.Submit(ctx, &agentpb.ConnectResponse{
		Id: &agentpb.InstructionId{Value: collectID},
		Instruction: &agentpb.ConnectResponse_CollectArtifact{CollectArtifact: &agentpb.CollectArtifact{
			ArtifactId: &agentpb.ArtifactId{Value: collectedArtifactID}, Path: agentDestination, Name: "collected",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	collectedPath := waitArtifact(t, service, collectedArtifactID)
	collected, err := os.ReadFile(collectedPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(collected, artifactContent) {
		t.Fatalf("collected = %q", collected)
	}
	digest := sha256.Sum256(artifactContent)
	progress, err := service.QueryArtifactUpload(ctx, &agentpb.QueryArtifactUploadRequest{
		InstructionId: &agentpb.InstructionId{Value: collectID}, ArtifactId: &agentpb.ArtifactId{Value: collectedArtifactID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !progress.GetComplete() || !bytes.Equal(progress.GetArtifact().GetSha256(), digest[:]) {
		t.Fatalf("collected progress = %#v", progress)
	}

	if err := service.Disconnect(); err != nil {
		t.Fatal(err)
	}
	reconnected := waitMatchingEvent(t, service.Events(), func(event *agentpb.ConnectRequest) bool { return event.GetHello() != nil })
	if reconnected.GetHello().GetInstallationId().GetValue() != "bufconn-installation" {
		t.Fatalf("reconnect hello = %#v", reconnected)
	}
}

type testBearerCredentials struct {
	token string
}

func (c testBearerCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.token}, nil
}

func (testBearerCredentials) RequireTransportSecurity() bool {
	return false
}

func waitMatchingEvent(t *testing.T, events <-chan *agentpb.ConnectRequest, matches func(*agentpb.ConnectRequest) bool) *agentpb.ConnectRequest {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event := <-events:
			if matches(event) {
				return event
			}
		case <-timer.C:
			t.Fatal("timed out waiting for matching agent event")
			return nil
		}
	}
}

func waitArtifact(t *testing.T, service *Server, artifactID string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		path, err := service.ArtifactPath(artifactID)
		if err == nil {
			return path
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for collected artifact")
	return ""
}
