// Package execproc is the development Runtime: it "hosts" a container by
// running a local executable as a plain OS process. The image ref is the
// path to the executable. It exists so the full contour — server, agent,
// user worker — runs on one developer machine without root or an OCI
// runtime; production machines use the runc runtime.
package execproc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/graphene-ci/agent/pkg/host"
)

// Runtime runs one process per (machine × run) under dataDir.
type Runtime struct {
	dataDir string
}

// New creates the runtime rooted at dataDir.
func New(dataDir string) *Runtime {
	return &Runtime{dataDir: dataDir}
}

// Pull verifies the "image" — the executable — exists.
func (r *Runtime) Pull(_ context.Context, c host.RunContainer) error {
	image := c.Image
	info, err := os.Stat(string(image))
	if err != nil {
		return fmt.Errorf("image executable: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("image executable %q is a directory", image)
	}
	return nil
}

// Start launches the process if it is not already running.
func (r *Runtime) Start(ctx context.Context, c host.RunContainer) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if status, err := r.Status(ctx, c); err == nil && status == host.StatusRunning {
		return nil
	}
	dir := r.containerDir(c)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	logFile, err := os.OpenFile(filepath.Join(dir, "log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // path under the agent's own data dir
	if err != nil {
		return err
	}
	defer func() { _ = logFile.Close() }()

	workspace, err := r.workspaceDir(c)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(workspace, 0o755); err != nil { //nolint:gosec // shared work dir, see the runc runtime
		return err
	}
	cmd := exec.Command(string(c.Image)) //nolint:gosec // running the user's executable is this runtime's purpose
	cmd.Dir = dir
	cmd.Env = append(envList(c.Env), "GRAPHENE_WORKSPACE="+workspace)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// Own process group: stopping kills the whole tree, not just the leader.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %q: %w", c.Image, err)
	}
	pid := cmd.Process.Pid
	// The runtime supervises through Status polling, not through Wait;
	// still reap the child when it exits so it does not linger as a zombie.
	go func() { _ = cmd.Wait() }()
	return os.WriteFile(r.pidFile(c), []byte(strconv.Itoa(pid)), 0o600)
}

// Stop terminates the process tree; an absent container is not an error.
func (r *Runtime) Stop(ctx context.Context, c host.RunContainer) error {
	pid, err := r.readPid(c)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if alive(pid) {
		_ = syscall.Kill(-pid, syscall.SIGTERM)
		deadline := time.Now().Add(10 * time.Second)
		for alive(pid) && time.Now().Before(deadline) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		if alive(pid) {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
	}
	// The workspace dies with the run container.
	if workspace, err := r.workspaceDir(c); err == nil {
		_ = os.RemoveAll(workspace)
	}
	return os.RemoveAll(r.containerDir(c))
}

// Status reports the process state.
func (r *Runtime) Status(_ context.Context, c host.RunContainer) (host.ContainerStatus, error) {
	pid, err := r.readPid(c)
	if errors.Is(err, os.ErrNotExist) {
		return host.StatusStopped, nil
	}
	if err != nil {
		return host.StatusFailed, err
	}
	if alive(pid) {
		return host.StatusRunning, nil
	}
	return host.StatusStopped, nil
}

// LogPath names the process's combined output capture.
func (r *Runtime) LogPath(c host.RunContainer) string {
	return filepath.Join(r.containerDir(c), "log")
}

func (r *Runtime) containerDir(c host.RunContainer) string {
	return filepath.Join(r.dataDir, "containers", containerName(c))
}

// workspaceDir is the per-(machine × run) work directory; the process
// runs on the machine, so the path is trivially the same everywhere.
func (r *Runtime) workspaceDir(c host.RunContainer) (string, error) {
	return filepath.Abs(filepath.Join(r.dataDir, "work", containerName(c)))
}

func (r *Runtime) pidFile(c host.RunContainer) string {
	return filepath.Join(r.containerDir(c), "pid")
}

func (r *Runtime) readPid(c host.RunContainer) (int, error) {
	raw, err := os.ReadFile(r.pidFile(c))
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(raw)))
}

// containerName flattens the (machine × run) pair into one path- and
// runtime-safe name.
func containerName(c host.RunContainer) string {
	sanitize := func(s string) string {
		return strings.Map(func(r rune) rune {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.':
				return r
			default:
				return '_'
			}
		}, s)
	}
	return sanitize(string(c.AgentId)) + "--" + sanitize(string(c.RunId))
}

func envList(env map[string]string) []string {
	out := os.Environ()
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	return out
}

func alive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}
