package command

import (
	"context"
	"io"
	"strings"
	"sync"
	"syscall"
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
	resultChannel := make(chan Result, 1)
	go func() { resultChannel <- process.Wait() }()
	data := outputs()
	result := <-resultChannel
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

func TestNonTerminalInputAndClose(t *testing.T) {
	t.Parallel()
	process, err := Start(context.Background(), testConfig(), &agentpb.RunCommand{Command: "cat"})
	if err != nil {
		t.Fatal(err)
	}
	read := readOutputs(process.Readers())
	if err := process.Input([]byte("hello"), true); err != nil {
		t.Fatal(err)
	}
	if err := process.Input(nil, false); err == nil {
		t.Fatal("expected closed stdin error")
	}
	if result := process.Wait(); result.ExitCode != 0 || result.Err != nil {
		t.Fatalf("result = %#v", result)
	}
	if got := string(read()[agentpb.OutputStream_OUTPUT_STREAM_STDOUT]); got != "hello" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestResizeTerminalAndValidation(t *testing.T) {
	t.Parallel()
	terminal, err := Start(context.Background(), testConfig(), &agentpb.RunCommand{
		Command: "sleep 5", Terminal: true, TerminalSize: &agentpb.TerminalSize{Columns: 80, Rows: 24},
	})
	if err != nil {
		t.Fatal(err)
	}
	read := readOutputs(terminal.Readers())
	if err := terminal.Resize(&agentpb.TerminalSize{Columns: 100, Rows: 40}); err != nil {
		t.Fatal(err)
	}
	for _, size := range []*agentpb.TerminalSize{nil, {}, {Columns: 1 << 16, Rows: 1}} {
		if err := terminal.Resize(size); err == nil {
			t.Fatalf("Resize(%#v) succeeded", size)
		}
	}
	terminal.Cancel()
	_ = terminal.Wait()
	_ = read()
	_ = terminal.Close()

	plain, err := Start(context.Background(), testConfig(), &agentpb.RunCommand{Command: "sleep 5"})
	if err != nil {
		t.Fatal(err)
	}
	plainRead := readOutputs(plain.Readers())
	if err := plain.Resize(&agentpb.TerminalSize{Columns: 80, Rows: 24}); err == nil {
		t.Fatal("non-terminal resize succeeded")
	}
	plain.Cancel()
	_ = plain.Wait()
	_ = plainRead()
}

func TestSignalAndCancel(t *testing.T) {
	t.Parallel()
	process, err := Start(context.Background(), testConfig(), &agentpb.RunCommand{Command: "sleep 5"})
	if err != nil {
		t.Fatal(err)
	}
	read := readOutputs(process.Readers())
	if err := process.Signal(agentpb.Signal_SIGNAL_UNSPECIFIED); err == nil {
		t.Fatal("unspecified signal succeeded")
	}
	if err := process.Signal(agentpb.Signal_SIGNAL_KILL); err != nil {
		t.Fatal(err)
	}
	result := process.Wait()
	_ = read()
	if result.Signal != agentpb.Signal_SIGNAL_KILL || result.ExitCode != 128+int32(syscall.SIGKILL) {
		t.Fatalf("result = %#v", result)
	}

	canceled, err := Start(context.Background(), testConfig(), &agentpb.RunCommand{Command: "sleep 5"})
	if err != nil {
		t.Fatal(err)
	}
	canceledRead := readOutputs(canceled.Readers())
	canceled.Cancel()
	if result := canceled.Wait(); !result.Canceled || result.TimedOut {
		t.Fatalf("canceled result = %#v", result)
	}
	_ = canceledRead()
}

func TestStartRejectsInvalidRequest(t *testing.T) {
	t.Parallel()
	invalidDuration := durationpb.New(time.Second)
	invalidDuration.Nanos = 1_000_000_000
	for _, test := range []struct {
		name    string
		request *agentpb.RunCommand
		config  Config
	}{
		{name: "absent", config: testConfig()},
		{name: "relative directory", request: &agentpb.RunCommand{WorkingDirectory: "tmp"}, config: testConfig()},
		{name: "nul directory", request: &agentpb.RunCommand{WorkingDirectory: "/tmp\x00"}, config: testConfig()},
		{name: "nul command", request: &agentpb.RunCommand{Command: "x\x00"}, config: testConfig()},
		{name: "invalid timeout", request: &agentpb.RunCommand{Timeout: invalidDuration}, config: testConfig()},
		{name: "negative timeout", request: &agentpb.RunCommand{Timeout: durationpb.New(-time.Second)}, config: testConfig()},
		{name: "bad environment name", request: &agentpb.RunCommand{Environment: map[string]string{"A=B": "x"}}, config: testConfig()},
		{name: "bad environment value", request: &agentpb.RunCommand{Environment: map[string]string{"A": "x\x00"}}, config: testConfig()},
		{name: "missing terminal size", request: &agentpb.RunCommand{Terminal: true}, config: testConfig()},
		{name: "zero terminal size", request: &agentpb.RunCommand{Terminal: true, TerminalSize: &agentpb.TerminalSize{}}, config: testConfig()},
		{name: "large terminal size", request: &agentpb.RunCommand{Terminal: true, TerminalSize: &agentpb.TerminalSize{Columns: 1 << 16, Rows: 1}}, config: testConfig()},
		{name: "missing shell", request: &agentpb.RunCommand{Command: "true"}, config: Config{Shell: "/does/not/exist", WorkingDirectory: "/", DefaultTimeout: time.Minute}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if process, err := Start(context.Background(), test.config, test.request); err == nil {
				process.Cancel()
				t.Fatalf("Start() succeeded: %#v", process)
			}
		})
	}
}

func TestPortableSignal(t *testing.T) {
	t.Parallel()
	for signal, want := range map[syscall.Signal]agentpb.Signal{
		syscall.SIGINT: agentpb.Signal_SIGNAL_INTERRUPT, syscall.SIGTERM: agentpb.Signal_SIGNAL_TERMINATE,
		syscall.SIGKILL: agentpb.Signal_SIGNAL_KILL, syscall.SIGHUP: agentpb.Signal_SIGNAL_UNSPECIFIED,
	} {
		if got := portableSignal(signal); got != want {
			t.Fatalf("portableSignal(%d) = %s, want %s", signal, got, want)
		}
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
