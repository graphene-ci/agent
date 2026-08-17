package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
)

type Config struct {
	Shell            string
	WorkingDirectory string
	DefaultTimeout   time.Duration
	TerminateGrace   time.Duration
}

type Result struct {
	ExitCode int32
	Signal   agentpb.Signal
	TimedOut bool
	Canceled bool
	Err      error
}

type Process struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	terminal *os.File
	readers  map[agentpb.OutputStream]io.Reader
	finished chan struct{}
	result   Result
	inputMu  sync.Mutex
	stopOnce sync.Once
	mu       sync.Mutex
	timedOut bool
	canceled bool
	grace    time.Duration
}

func Start(ctx context.Context, cfg Config, request *agentpb.RunCommand) (*Process, error) {
	if request == nil {
		return nil, errors.New("RunCommand is absent")
	}
	workingDirectory := request.GetWorkingDirectory()
	if workingDirectory == "" {
		workingDirectory = cfg.WorkingDirectory
	}
	if !filepath.IsAbs(workingDirectory) {
		return nil, errors.New("working_directory must be absolute")
	}
	if strings.IndexByte(workingDirectory, 0) >= 0 || strings.IndexByte(request.GetCommand(), 0) >= 0 {
		return nil, errors.New("command and working_directory must not contain NUL")
	}
	timeout, err := commandTimeout(request, cfg.DefaultTimeout)
	if err != nil {
		return nil, err
	}
	environment, err := mergeEnvironment(request.GetEnvironment())
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(cfg.Shell, "-c", request.GetCommand())
	cmd.Dir = workingDirectory
	cmd.Env = environment
	process := &Process{cmd: cmd, finished: make(chan struct{}), grace: cfg.TerminateGrace}

	if request.GetTerminal() {
		if request.GetTerminalSize() == nil || request.GetTerminalSize().GetColumns() == 0 || request.GetTerminalSize().GetRows() == 0 {
			return nil, errors.New("terminal_size columns and rows must be positive")
		}
		if request.GetTerminalSize().GetColumns() > uint32(^uint16(0)) || request.GetTerminalSize().GetRows() > uint32(^uint16(0)) {
			return nil, errors.New("terminal_size exceeds uint16")
		}
		terminal, startErr := pty.StartWithSize(cmd, &pty.Winsize{Cols: uint16(request.GetTerminalSize().GetColumns()), Rows: uint16(request.GetTerminalSize().GetRows())})
		if startErr != nil {
			return nil, startErr
		}
		process.stdin = terminal
		process.terminal = terminal
		process.readers = map[agentpb.OutputStream]io.Reader{agentpb.OutputStream_OUTPUT_STREAM_TERMINAL: terminal}
	} else {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		stdin, pipeErr := cmd.StdinPipe()
		if pipeErr != nil {
			return nil, fmt.Errorf("create stdin pipe: %w", pipeErr)
		}
		stdout, pipeErr := cmd.StdoutPipe()
		if pipeErr != nil {
			return nil, fmt.Errorf("create stdout pipe: %w", pipeErr)
		}
		stderr, pipeErr := cmd.StderrPipe()
		if pipeErr != nil {
			return nil, fmt.Errorf("create stderr pipe: %w", pipeErr)
		}
		if startErr := cmd.Start(); startErr != nil {
			return nil, startErr
		}
		process.stdin = stdin
		process.readers = map[agentpb.OutputStream]io.Reader{
			agentpb.OutputStream_OUTPUT_STREAM_STDOUT: stdout,
			agentpb.OutputStream_OUTPUT_STREAM_STDERR: stderr,
		}
	}

	go process.wait()
	go func() {
		select {
		case <-ctx.Done():
			process.Cancel()
		case <-process.finished:
		}
	}()
	if timeout > 0 {
		go func() {
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				process.timeout()
			case <-ctx.Done():
			case <-process.finished:
			}
		}()
	}
	return process, nil
}

func (p *Process) PID() uint64 {
	if p.cmd.Process == nil || p.cmd.Process.Pid < 0 {
		return 0
	}
	return uint64(p.cmd.Process.Pid)
}

func (p *Process) Readers() map[agentpb.OutputStream]io.Reader {
	return p.readers
}

func (p *Process) Input(data []byte, closeInput bool) error {
	p.inputMu.Lock()
	defer p.inputMu.Unlock()
	if p.stdin == nil {
		return errors.New("stdin is closed")
	}
	if len(data) > 0 {
		if _, err := p.stdin.Write(data); err != nil {
			return fmt.Errorf("write stdin: %w", err)
		}
	}
	if !closeInput {
		return nil
	}
	if p.terminal != nil {
		_, err := p.stdin.Write([]byte{0x04})
		return err
	}
	err := p.stdin.Close()
	p.stdin = nil
	return err
}

func (p *Process) Resize(size *agentpb.TerminalSize) error {
	if p.terminal == nil {
		return errors.New("command has no terminal")
	}
	if size == nil || size.GetColumns() == 0 || size.GetRows() == 0 {
		return errors.New("terminal size must be positive")
	}
	if size.GetColumns() > uint32(^uint16(0)) || size.GetRows() > uint32(^uint16(0)) {
		return errors.New("terminal size exceeds uint16")
	}
	return pty.Setsize(p.terminal, &pty.Winsize{Cols: uint16(size.GetColumns()), Rows: uint16(size.GetRows())})
}

func (p *Process) Signal(signal agentpb.Signal) error {
	var operatingSystemSignal syscall.Signal
	switch signal {
	case agentpb.Signal_SIGNAL_INTERRUPT:
		operatingSystemSignal = syscall.SIGINT
	case agentpb.Signal_SIGNAL_TERMINATE:
		operatingSystemSignal = syscall.SIGTERM
	case agentpb.Signal_SIGNAL_KILL:
		operatingSystemSignal = syscall.SIGKILL
	default:
		return errors.New("signal is unspecified")
	}
	return signalGroup(p.cmd.Process.Pid, operatingSystemSignal)
}

func (p *Process) Cancel() {
	p.mu.Lock()
	p.canceled = true
	p.mu.Unlock()
	p.stop()
}

func (p *Process) timeout() {
	p.mu.Lock()
	p.timedOut = true
	p.mu.Unlock()
	p.stop()
}

func (p *Process) stop() {
	p.stopOnce.Do(func() {
		_ = signalGroup(p.cmd.Process.Pid, syscall.SIGTERM)
		go func() {
			timer := time.NewTimer(p.grace)
			defer timer.Stop()
			select {
			case <-timer.C:
				_ = signalGroup(p.cmd.Process.Pid, syscall.SIGKILL)
			case <-p.finished:
			}
		}()
	})
}

func (p *Process) Wait() Result {
	<-p.finished
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.result
}

func (p *Process) Close() error {
	if p.terminal != nil {
		return p.terminal.Close()
	}
	return nil
}

func (p *Process) wait() {
	err := p.cmd.Wait()
	result := Result{}
	var exitError *exec.ExitError
	if err != nil && !errors.As(err, &exitError) {
		result.Err = err
	}
	p.mu.Lock()
	result.TimedOut = p.timedOut
	result.Canceled = p.canceled
	p.mu.Unlock()
	if p.cmd.ProcessState != nil {
		if status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok {
			if status.Exited() {
				result.ExitCode = int32(status.ExitStatus())
			} else if status.Signaled() {
				result.ExitCode = int32(128 + status.Signal())
				result.Signal = portableSignal(status.Signal())
			}
		}
	}
	p.mu.Lock()
	p.result = result
	p.mu.Unlock()
	close(p.finished)
}

func commandTimeout(request *agentpb.RunCommand, defaultTimeout time.Duration) (time.Duration, error) {
	if request.GetTimeout() == nil {
		return defaultTimeout, nil
	}
	if err := request.GetTimeout().CheckValid(); err != nil {
		return 0, fmt.Errorf("invalid command timeout: %w", err)
	}
	duration := request.GetTimeout().AsDuration()
	if duration < 0 {
		return 0, errors.New("command timeout must not be negative")
	}
	if duration == 0 {
		return defaultTimeout, nil
	}
	return duration, nil
}

func mergeEnvironment(overrides map[string]string) ([]string, error) {
	values := make(map[string]string)
	for _, entry := range os.Environ() {
		name, value, ok := strings.Cut(entry, "=")
		if ok {
			values[name] = value
		}
	}
	for name, value := range overrides {
		if name == "" || strings.ContainsAny(name, "=\x00") || strings.IndexByte(value, 0) >= 0 {
			return nil, fmt.Errorf("invalid environment variable %q", name)
		}
		values[name] = value
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]string, 0, len(names))
	for _, name := range names {
		result = append(result, name+"="+values[name])
	}
	return result, nil
}

func signalGroup(pid int, signal syscall.Signal) error {
	err := syscall.Kill(-pid, signal)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

func portableSignal(signal syscall.Signal) agentpb.Signal {
	switch signal {
	case syscall.SIGINT:
		return agentpb.Signal_SIGNAL_INTERRUPT
	case syscall.SIGTERM:
		return agentpb.Signal_SIGNAL_TERMINATE
	case syscall.SIGKILL:
		return agentpb.Signal_SIGNAL_KILL
	default:
		return agentpb.Signal_SIGNAL_UNSPECIFIED
	}
}
