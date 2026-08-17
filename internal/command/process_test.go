package command

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestNonTerminalCommandSeparatesOutputAndExitCode(t *testing.T) {
	t.Parallel()
	process, err := Start(context.Background(), testConfig(), &agentpb.RunCommand{Command: "printf stdout; printf stderr >&2; exit 7"})
	if err != nil {
		t.Fatal(err)
	}
	outputs := readOutputs(process.Readers())
	result := process.Wait()
	data := outputs()
	if string(data[agentpb.OutputStream_OUTPUT_STREAM_STDOUT]) != "stdout" {
		t.Fatalf("stdout = %q", data[agentpb.OutputStream_OUTPUT_STREAM_STDOUT])
	}
	if string(data[agentpb.OutputStream_OUTPUT_STREAM_STDERR]) != "stderr" {
		t.Fatalf("stderr = %q", data[agentpb.OutputStream_OUTPUT_STREAM_STDERR])
	}
	if result.ExitCode != 7 || result.TimedOut || result.Canceled || result.Err != nil {
		t.Fatalf("result = %#v", result)
	}
}

func TestCommandTimeoutTerminatesProcessGroup(t *testing.T) {
	t.Parallel()
	process, err := Start(context.Background(), testConfig(), &agentpb.RunCommand{
		Command: "sleep 5",
		Timeout: durationpb.New(50 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	read := readOutputs(process.Readers())
	result := process.Wait()
	_ = read()
	if !result.TimedOut || result.Canceled {
		t.Fatalf("result = %#v", result)
	}
}

func TestTerminalInputAndOutput(t *testing.T) {
	t.Parallel()
	process, err := Start(context.Background(), testConfig(), &agentpb.RunCommand{
		Command:      "read value; printf 'got:%s\\n' \"$value\"",
		Terminal:     true,
		TerminalSize: &agentpb.TerminalSize{Columns: 80, Rows: 24},
	})
	if err != nil {
		t.Fatal(err)
	}
	read := readOutputs(process.Readers())
	if err := process.Input([]byte("hello\n"), false); err != nil {
		t.Fatal(err)
	}
	result := process.Wait()
	data := read()
	if result.ExitCode != 0 {
		t.Fatalf("result = %#v", result)
	}
	if !strings.Contains(string(data[agentpb.OutputStream_OUTPUT_STREAM_TERMINAL]), "got:hello") {
		t.Fatalf("terminal output = %q", data[agentpb.OutputStream_OUTPUT_STREAM_TERMINAL])
	}
	if err := process.Close(); err != nil {
		t.Fatal(err)
	}
}

func testConfig() Config {
	return Config{Shell: "/bin/sh", WorkingDirectory: "/", DefaultTimeout: time.Minute, TerminateGrace: 10 * time.Millisecond}
}

func readOutputs(readers map[agentpb.OutputStream]io.Reader) func() map[agentpb.OutputStream][]byte {
	var wait sync.WaitGroup
	result := make(map[agentpb.OutputStream][]byte)
	var mu sync.Mutex
	for stream, reader := range readers {
		wait.Add(1)
		go func(stream agentpb.OutputStream, reader io.Reader) {
			defer wait.Done()
			data, _ := io.ReadAll(reader)
			mu.Lock()
			result[stream] = data
			mu.Unlock()
		}(stream, reader)
	}
	return func() map[agentpb.OutputStream][]byte {
		wait.Wait()
		return result
	}
}
