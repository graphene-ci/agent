package agent

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/graphene-ci/agent/internal/command"
	"github.com/graphene-ci/agent/internal/facts"
	"github.com/graphene-ci/agent/internal/state"
	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
)

type captureSender struct {
	control chan *agentpb.ConnectRequest
	mu      sync.Mutex
	output  []*agentpb.ConnectRequest
}

func (s *captureSender) Control(_ context.Context, request *agentpb.ConnectRequest) error {
	s.control <- request
	return nil
}

func (s *captureSender) Output(_ context.Context, request *agentpb.ConnectRequest) error {
	s.mu.Lock()
	s.output = append(s.output, request)
	s.mu.Unlock()
	return nil
}

func TestRunCommandAndRejectDuplicateInstruction(t *testing.T) {
	t.Parallel()
	store, err := state.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	sender := &captureSender{control: make(chan *agentpb.ConnectRequest, 16)}
	runtimeAgent := New(store, sender, facts.New(facts.Config{Timeout: time.Second}), nil, Config{
		Command:      command.Config{Shell: "/bin/sh", WorkingDirectory: "/", DefaultTimeout: time.Minute, TerminateGrace: 10 * time.Millisecond},
		GlobalOutput: 1 << 20, OutputDrain: time.Second, MaxConcurrent: 2,
	})
	request := &agentpb.ConnectResponse{
		Id:          &agentpb.InstructionId{Value: "instruction-1"},
		Instruction: &agentpb.ConnectResponse_RunCommand{RunCommand: &agentpb.RunCommand{Command: "printf hello"}},
	}
	if err := runtimeAgent.Handle(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	started := waitControl(t, sender.control)
	if started.GetCommandStarted() == nil {
		t.Fatalf("first event = %#v", started)
	}
	exited := waitControl(t, sender.control)
	if exited.GetCommandExited() == nil || exited.GetCommandExited().GetExitCode() != 0 {
		t.Fatalf("second event = %#v", exited)
	}
	if err := runtimeAgent.Handle(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	select {
	case duplicate := <-sender.control:
		t.Fatalf("duplicate event = %#v", duplicate)
	case <-time.After(50 * time.Millisecond):
	}
	sender.mu.Lock()
	defer sender.mu.Unlock()
	if len(sender.output) != 1 || string(sender.output[0].GetCommandOutput().GetData()) != "hello" {
		t.Fatalf("output = %#v", sender.output)
	}
}

func TestReadFactsReturnsTypedGroup(t *testing.T) {
	t.Parallel()
	store, err := state.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	sender := &captureSender{control: make(chan *agentpb.ConnectRequest, 4)}
	runtimeAgent := New(store, sender, facts.New(facts.Config{Timeout: 5 * time.Second}), nil, Config{
		Command:      command.Config{Shell: "/bin/sh", WorkingDirectory: "/", DefaultTimeout: time.Minute, TerminateGrace: time.Second},
		GlobalOutput: 1 << 20, OutputDrain: time.Second, MaxConcurrent: 1,
	})
	request := &agentpb.ConnectResponse{
		Id: &agentpb.InstructionId{Value: "facts-1"},
		Instruction: &agentpb.ConnectResponse_ReadFacts{ReadFacts: &agentpb.ReadFacts{Groups: []agentpb.FactGroup{
			agentpb.FactGroup_FACT_GROUP_MEMORY,
		}}},
	}
	if err := runtimeAgent.Handle(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	event := waitControl(t, sender.control)
	observed := event.GetFactsRead()
	if observed == nil || observed.GetObservedAt() == nil || len(observed.GetResults()) != 1 {
		t.Fatalf("facts event = %#v", event)
	}
	group := observed.GetResults()[0]
	if group.GetGroup() != agentpb.FactGroup_FACT_GROUP_MEMORY || group.GetMemory() == nil ||
		group.GetStatus() == agentpb.FactStatus_FACT_STATUS_UNSPECIFIED {
		t.Fatalf("fact group = %#v", group)
	}
}

func waitControl(t *testing.T, events <-chan *agentpb.ConnectRequest) *agentpb.ConnectRequest {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for control event")
		return nil
	}
}
