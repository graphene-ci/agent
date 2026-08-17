package testserver

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConsoleSubmitsEveryInstruction(t *testing.T) {
	t.Parallel()
	service, client, stop := testService(t)
	defer stop()
	ctx := authenticatedContext(testToken)
	stream := connectAgent(t, ctx, client, "console-agent")
	_ = waitEvent(t, service.Events())
	output := &bytes.Buffer{}
	console := NewConsole(service, output)

	commands := []struct {
		line  string
		check func(*testing.T, *agentpb.ConnectResponse)
	}{
		{line: "command printf hello", check: func(t *testing.T, response *agentpb.ConnectResponse) {
			if response.GetRunCommand().GetCommand() != "printf hello" || response.GetRunCommand().GetTerminal() {
				t.Fatalf("response = %#v", response)
			}
		}},
		{line: "terminal sh", check: func(t *testing.T, response *agentpb.ConnectResponse) {
			command := response.GetRunCommand()
			if !command.GetTerminal() || command.GetTerminalSize().GetColumns() != 80 || command.GetTerminalSize().GetRows() != 24 {
				t.Fatalf("response = %#v", response)
			}
		}},
		{line: "facts sensitive", check: func(t *testing.T, response *agentpb.ConnectResponse) {
			if !response.GetReadFacts().GetIncludeSensitive() {
				t.Fatalf("response = %#v", response)
			}
		}},
		{line: "collect /tmp/source result", check: func(t *testing.T, response *agentpb.ConnectResponse) {
			if response.GetCollectArtifact().GetPath() != "/tmp/source" || response.GetCollectArtifact().GetName() != "result" {
				t.Fatalf("response = %#v", response)
			}
		}},
		{line: "input command-id hello world", check: func(t *testing.T, response *agentpb.ConnectResponse) {
			if response.GetId().GetValue() != "command-id" || string(response.GetCommandInput().GetData()) != "hello world" {
				t.Fatalf("response = %#v", response)
			}
		}},
		{line: "close-input command-id", check: func(t *testing.T, response *agentpb.ConnectResponse) {
			if !response.GetCommandInput().GetClose() {
				t.Fatalf("response = %#v", response)
			}
		}},
		{line: "resize command-id 120 40", check: func(t *testing.T, response *agentpb.ConnectResponse) {
			if response.GetResizeTerminal().GetSize().GetColumns() != 120 || response.GetResizeTerminal().GetSize().GetRows() != 40 {
				t.Fatalf("response = %#v", response)
			}
		}},
		{line: "signal command-id terminate", check: func(t *testing.T, response *agentpb.ConnectResponse) {
			if response.GetSignalCommand().GetSignal() != agentpb.Signal_SIGNAL_TERMINATE {
				t.Fatalf("response = %#v", response)
			}
		}},
		{line: "cancel command-id", check: func(t *testing.T, response *agentpb.ConnectResponse) {
			if response.GetCancelOperation() == nil {
				t.Fatalf("response = %#v", response)
			}
		}},
		{line: "ping", check: func(t *testing.T, response *agentpb.ConnectResponse) {
			if response.GetPing() == nil || response.GetId() != nil {
				t.Fatalf("response = %#v", response)
			}
		}},
	}
	for _, test := range commands {
		if err := console.Execute(ctx, test.line); err != nil {
			t.Fatalf("Execute(%q): %v", test.line, err)
		}
		response, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		test.check(t, response)
	}

	source := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(source, []byte("artifact"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := console.Execute(ctx, "put "+source+" /tmp/destination"); err != nil {
		t.Fatal(err)
	}
	if response, err := stream.Recv(); err != nil || response.GetPutArtifact().GetSize() != 8 {
		t.Fatalf("put response = %#v, %v", response, err)
	}
	if err := console.Execute(ctx, "help"); err != nil {
		t.Fatal(err)
	}
	consoleText := consoleOutput(console, output)
	if !strings.Contains(consoleText, "commands") || !strings.Contains(consoleText, "submitted") {
		t.Fatalf("console output = %q", consoleText)
	}
	if err := console.Execute(ctx, "quit"); !errors.Is(err, ErrQuit) {
		t.Fatalf("quit error = %v", err)
	}
	if err := console.Execute(ctx, "disconnect"); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("disconnect error = %v", err)
	}
}

func TestConsoleRejectsInvalidCommands(t *testing.T) {
	t.Parallel()
	service, err := New(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	console := NewConsole(service, &bytes.Buffer{})
	for _, line := range []string{
		"", "unknown", "command", "terminal", "facts other", "facts 'unterminated", "put one",
		"collect", "collect one two three", "input id", "close-input", "resize id 0 1",
		"resize id 1 nope", "signal id stop", "cancel", "disconnect",
	} {
		if err := console.Execute(context.Background(), line); err == nil {
			t.Fatalf("Execute(%q) succeeded", line)
		}
	}
}

func TestConsoleRunWritesErrorsAndEvents(t *testing.T) {
	t.Parallel()
	service, err := New(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output := &bytes.Buffer{}
	console := NewConsole(service, output)
	service.events <- &agentpb.ConnectRequest{Event: &agentpb.ConnectRequest_Pong{Pong: &agentpb.Pong{}}}
	if err := console.Run(ctx, strings.NewReader("# comment\n\nunknown\nhelp\nexit\n")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for !strings.Contains(consoleOutput(console, output), "pong") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	consoleText := consoleOutput(console, output)
	if !strings.Contains(consoleText, "unknown command") || !strings.Contains(consoleText, "commands") || !strings.Contains(consoleText, "pong") {
		t.Fatalf("output = %q", consoleText)
	}
}

func consoleOutput(console *Console, output *bytes.Buffer) string {
	console.mu.Lock()
	defer console.mu.Unlock()
	return output.String()
}

func TestParseConsoleValues(t *testing.T) {
	t.Parallel()
	for value, want := range map[string]uint32{"1": 1, "4294967295": 1<<32 - 1} {
		if got, err := parseUint32(value); err != nil || got != want {
			t.Fatalf("parseUint32(%q) = %d, %v", value, got, err)
		}
	}
	for _, value := range []string{"0", "-1", "4294967296", "bad"} {
		if _, err := parseUint32(value); err == nil {
			t.Fatalf("parseUint32(%q) succeeded", value)
		}
	}
	for value, want := range map[string]agentpb.Signal{
		"interrupt": agentpb.Signal_SIGNAL_INTERRUPT,
		"terminate": agentpb.Signal_SIGNAL_TERMINATE,
		"kill":      agentpb.Signal_SIGNAL_KILL,
	} {
		if got, err := parseSignal(value); err != nil || got != want {
			t.Fatalf("parseSignal(%q) = %s, %v", value, got, err)
		}
	}
	if _, err := parseSignal("bad"); err == nil {
		t.Fatal("invalid signal succeeded")
	}
}
