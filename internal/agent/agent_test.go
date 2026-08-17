package agent

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/graphene-ci/agent/internal/artifact"
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
		t.Fatalf("second event = %#v, failure = %#v", exited, exited.GetOperationFailed())
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

func TestHandleRejectsInvalidAndInactiveInstructions(t *testing.T) {
	t.Parallel()
	runtimeAgent, cleanup := testAgent(t, 1)
	defer cleanup()
	for _, response := range []*agentpb.ConnectResponse{
		nil,
		{},
		{Id: &agentpb.InstructionId{}, Instruction: &agentpb.ConnectResponse_Ping{Ping: &agentpb.Ping{}}},
		{Id: &agentpb.InstructionId{Value: "ping"}, Instruction: &agentpb.ConnectResponse_Ping{Ping: &agentpb.Ping{}}},
		{Id: &agentpb.InstructionId{Value: "missing"}, Instruction: &agentpb.ConnectResponse_CommandInput{CommandInput: &agentpb.CommandInput{}}},
		{Id: &agentpb.InstructionId{Value: "missing"}, Instruction: &agentpb.ConnectResponse_ResizeTerminal{ResizeTerminal: &agentpb.ResizeTerminal{}}},
		{Id: &agentpb.InstructionId{Value: "missing"}, Instruction: &agentpb.ConnectResponse_SignalCommand{SignalCommand: &agentpb.SignalCommand{}}},
	} {
		if err := runtimeAgent.Handle(context.Background(), response); err == nil {
			t.Fatalf("Handle(%#v) succeeded", response)
		}
	}
	if err := runtimeAgent.Handle(context.Background(), &agentpb.ConnectResponse{
		Id: &agentpb.InstructionId{Value: "missing"}, Instruction: &agentpb.ConnectResponse_CancelOperation{CancelOperation: &agentpb.CancelOperation{}},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCommandInputSignalAndCancel(t *testing.T) {
	t.Parallel()
	t.Run("input", func(t *testing.T) {
		runtimeAgent, sender, cleanup := testAgentWithSender(t, 1)
		defer cleanup()
		id := &agentpb.InstructionId{Value: "input-command"}
		if err := runtimeAgent.Handle(context.Background(), &agentpb.ConnectResponse{
			Id: id, Instruction: &agentpb.ConnectResponse_RunCommand{RunCommand: &agentpb.RunCommand{Command: "read value; printf %s \"$value\""}},
		}); err != nil {
			t.Fatal(err)
		}
		if event := waitControl(t, sender.control); event.GetCommandStarted() == nil {
			t.Fatalf("event = %#v", event)
		}
		if err := runtimeAgent.Handle(context.Background(), &agentpb.ConnectResponse{
			Id: id, Instruction: &agentpb.ConnectResponse_CommandInput{CommandInput: &agentpb.CommandInput{Data: []byte("hello\n"), Close: true}},
		}); err != nil {
			t.Fatal(err)
		}
		if event := waitControl(t, sender.control); event.GetCommandExited() == nil {
			t.Fatalf("event = %#v", event)
		}
		sender.mu.Lock()
		defer sender.mu.Unlock()
		if len(sender.output) != 1 || string(sender.output[0].GetCommandOutput().GetData()) != "hello" {
			t.Fatalf("output = %#v", sender.output)
		}
	})

	t.Run("signal", func(t *testing.T) {
		runtimeAgent, sender, cleanup := testAgentWithSender(t, 1)
		defer cleanup()
		id := &agentpb.InstructionId{Value: "signal-command"}
		if err := runtimeAgent.Handle(context.Background(), &agentpb.ConnectResponse{
			Id: id, Instruction: &agentpb.ConnectResponse_RunCommand{RunCommand: &agentpb.RunCommand{Command: "sleep 5"}},
		}); err != nil {
			t.Fatal(err)
		}
		_ = waitControl(t, sender.control)
		if err := runtimeAgent.Handle(context.Background(), &agentpb.ConnectResponse{
			Id: id, Instruction: &agentpb.ConnectResponse_SignalCommand{SignalCommand: &agentpb.SignalCommand{Signal: agentpb.Signal_SIGNAL_KILL}},
		}); err != nil {
			t.Fatal(err)
		}
		if event := waitControl(t, sender.control); event.GetCommandExited().GetSignal() != agentpb.Signal_SIGNAL_KILL {
			t.Fatalf("event = %#v", event)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		runtimeAgent, sender, cleanup := testAgentWithSender(t, 1)
		defer cleanup()
		id := &agentpb.InstructionId{Value: "cancel-command"}
		if err := runtimeAgent.Handle(context.Background(), &agentpb.ConnectResponse{
			Id: id, Instruction: &agentpb.ConnectResponse_RunCommand{RunCommand: &agentpb.RunCommand{Command: "sleep 5"}},
		}); err != nil {
			t.Fatal(err)
		}
		_ = waitControl(t, sender.control)
		if err := runtimeAgent.Handle(context.Background(), &agentpb.ConnectResponse{
			Id: id, Instruction: &agentpb.ConnectResponse_CancelOperation{CancelOperation: &agentpb.CancelOperation{}},
		}); err != nil {
			t.Fatal(err)
		}
		if event := waitControl(t, sender.control); event.GetOperationFailed().GetCode() != agentpb.ErrorCode_ERROR_CODE_CANCELED {
			t.Fatalf("event = %#v", event)
		}
	})
}

func TestInvalidResizeFailsCommand(t *testing.T) {
	t.Parallel()
	runtimeAgent, sender, cleanup := testAgentWithSender(t, 1)
	defer cleanup()
	id := &agentpb.InstructionId{Value: "resize-command"}
	if err := runtimeAgent.Handle(context.Background(), &agentpb.ConnectResponse{
		Id: id, Instruction: &agentpb.ConnectResponse_RunCommand{RunCommand: &agentpb.RunCommand{Command: "sleep 5"}},
	}); err != nil {
		t.Fatal(err)
	}
	_ = waitControl(t, sender.control)
	if err := runtimeAgent.Handle(context.Background(), &agentpb.ConnectResponse{
		Id: id, Instruction: &agentpb.ConnectResponse_ResizeTerminal{ResizeTerminal: &agentpb.ResizeTerminal{Size: &agentpb.TerminalSize{Columns: 80, Rows: 24}}},
	}); err != nil {
		t.Fatal(err)
	}
	if event := waitControl(t, sender.control); event.GetOperationFailed().GetCode() != agentpb.ErrorCode_ERROR_CODE_IO {
		t.Fatalf("event = %#v", event)
	}
}

func TestControlQueueFullAndCommandNotStarted(t *testing.T) {
	t.Parallel()
	runtimeAgent, cleanup := testAgent(t, 1)
	defer cleanup()
	operation := &operation{id: "command", kind: "command", controls: make(chan commandControl, 1)}
	runtimeAgent.active[operation.id] = operation
	if err := runtimeAgent.control(operation.id, commandControl{input: &agentpb.CommandInput{}}); err != nil {
		t.Fatal(err)
	}
	if err := runtimeAgent.control(operation.id, commandControl{input: &agentpb.CommandInput{}}); err == nil {
		t.Fatal("full control queue accepted a message")
	}
	failures := make(chan error, 1)
	commandControls(context.Background(), operation, failures)
	if err := <-failures; err == nil {
		t.Fatal("missing process did not fail")
	}
	operation.kind = "facts"
	if err := runtimeAgent.control(operation.id, commandControl{}); err == nil {
		t.Fatal("non-command operation accepted control")
	}
}

func TestMaximumConcurrentInstructionsFailsReservedOperation(t *testing.T) {
	t.Parallel()
	runtimeAgent, sender, cleanup := testAgentWithSender(t, 1)
	defer cleanup()
	first := &agentpb.ConnectResponse{Id: &agentpb.InstructionId{Value: "first"}, Instruction: &agentpb.ConnectResponse_RunCommand{RunCommand: &agentpb.RunCommand{Command: "sleep 5"}}}
	if err := runtimeAgent.Handle(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	_ = waitControl(t, sender.control)
	second := &agentpb.ConnectResponse{Id: &agentpb.InstructionId{Value: "second"}, Instruction: &agentpb.ConnectResponse_RunCommand{RunCommand: &agentpb.RunCommand{Command: "true"}}}
	if err := runtimeAgent.Handle(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if event := waitControl(t, sender.control); event.GetInstructionId().GetValue() != "second" || event.GetOperationFailed().GetCode() != agentpb.ErrorCode_ERROR_CODE_INTERNAL {
		t.Fatalf("event = %#v", event)
	}
	if err := runtimeAgent.cancel("first"); err != nil {
		t.Fatal(err)
	}
	_ = waitControl(t, sender.control)
}

func TestFailureCodeAndFail(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		err  error
		want agentpb.ErrorCode
	}{
		{err: artifact.ErrInvalidArgument, want: agentpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{err: artifact.ErrChecksumMismatch, want: agentpb.ErrorCode_ERROR_CODE_CHECKSUM_MISMATCH},
		{err: artifact.ErrAlreadyExists, want: agentpb.ErrorCode_ERROR_CODE_ALREADY_EXISTS},
		{err: context.Canceled, want: agentpb.ErrorCode_ERROR_CODE_CANCELED},
		{err: errors.New("io"), want: agentpb.ErrorCode_ERROR_CODE_IO},
	} {
		if got := failureCode(test.err); got != test.want {
			t.Fatalf("failureCode(%v) = %s", test.err, got)
		}
	}
	runtimeAgent, sender, cleanup := testAgentWithSender(t, 1)
	defer cleanup()
	reserved, err := runtimeAgent.store.Reserve("failed", "test")
	if err != nil || !reserved {
		t.Fatalf("Reserve() = %t, %v", reserved, err)
	}
	runtimeAgent.fail(context.Background(), "failed", agentpb.ErrorCode_ERROR_CODE_IO, "broken")
	if event := waitControl(t, sender.control); event.GetOperationFailed().GetMessage() != "broken" {
		t.Fatalf("event = %#v", event)
	}
}

func testAgent(t *testing.T, maxConcurrent int) (*Agent, func()) {
	t.Helper()
	runtimeAgent, _, cleanup := testAgentWithSender(t, maxConcurrent)
	return runtimeAgent, cleanup
}

func testAgentWithSender(t *testing.T, maxConcurrent int) (*Agent, *captureSender, func()) {
	t.Helper()
	store, err := state.Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	sender := &captureSender{control: make(chan *agentpb.ConnectRequest, 32)}
	runtimeAgent := New(store, sender, facts.New(facts.Config{Timeout: time.Second}), nil, Config{
		Command:      command.Config{Shell: "/bin/sh", WorkingDirectory: "/", DefaultTimeout: time.Minute, TerminateGrace: 10 * time.Millisecond},
		GlobalOutput: 1 << 20, OutputDrain: time.Second, MaxConcurrent: maxConcurrent,
	})
	return runtimeAgent, sender, func() { _ = store.Close() }
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
