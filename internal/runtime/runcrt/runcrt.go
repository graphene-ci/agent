// Package runcrt is the production Runtime: it hosts a container by
// pulling the OCI image through the server's registry proxy, unpacking a
// flattened rootfs, and running it with runc — no docker, no daemon. The
// container shares the host network (user code exists to change the
// machine and must reach the server); pid/mount/ipc/uts namespaces are
// its own. Requires root and a runc binary — both are the agent
// installer's job.
package runcrt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/graphene-ci/agent/pkg/host"
)

// Runtime drives runc under dataDir.
type Runtime struct {
	dataDir  string
	registry string
	insecure bool       // host[:port] of the server's registry proxy
	token    string     // scoped token: registry proxy auth
	runc     string     // runc binary path
	opLog    host.OpLog // raw operation output → obs; nil disables
}

// Options tune the runtime.
type Options struct {
	// Registry is the host[:port] of the server's registry proxy — the
	// only registry the agent pulls from.
	Registry string
	// Token authenticates the agent to the registry proxy.
	Token string
	// Insecure pulls over plain HTTP — the door without TLS (dev).
	Insecure bool
	// RuncBinary overrides the runc path; empty means "runc" from PATH.
	RuncBinary string
	// OpLog receives the raw output of pull/runc so an operator sees the
	// operation as if run by hand. Nil disables (tests).
	OpLog host.OpLog
}

// New creates the runtime rooted at dataDir.
func New(dataDir string, opts Options) *Runtime {
	if opts.RuncBinary == "" {
		opts.RuncBinary = "runc"
	}
	return &Runtime{
		dataDir:  dataDir,
		insecure: opts.Insecure,
		registry: opts.Registry,
		token:    opts.Token,
		runc:     opts.RuncBinary,
		opLog:    opts.OpLog,
	}
}

// op ships one raw line of a machine operation to obs, best-effort.
func (r *Runtime) op(c host.RunContainer, stream, line string) {
	if r.opLog != nil && line != "" {
		r.opLog.Op(c.AgentId, c.RunId, stream, line)
	}
}

// Start brings the container up: overlay-mounts the pulled image rootfs,
// writes the OCI spec, and runs it detached. Starting a running
// container is a no-op.
func (r *Runtime) Start(ctx context.Context, c host.RunContainer) error {
	if err := c.Validate(); err != nil {
		return err
	}
	name := containerName(c)
	// Idempotent (re)start. runc `run` refuses a name that already has
	// state — even a STOPPED one — with "container with given ID already
	// exists". A run-worker that died on its first attempt (say, the
	// server address was unreachable) leaves exactly that stale state, so
	// every retry hit "already exists" forever. A live container is a
	// no-op; a stale one is cleared before the run.
	if st, err := r.state(ctx, name); err == nil {
		if st == "running" || st == "created" {
			return nil
		}
		_ = exec.CommandContext(ctx, r.runc, "delete", "-f", name).Run() //nolint:gosec // see below
	}
	cfg, err := r.readImageConfig(c.Image)
	if err != nil {
		return err
	}
	bundle := r.bundleDir(c)
	merged := filepath.Join(bundle, "merged")
	for _, d := range []string{merged, filepath.Join(bundle, "upper"), filepath.Join(bundle, "work")} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			return err
		}
	}
	lower := filepath.Join(r.imageDir(c.Image), "rootfs")
	if err := mountOverlay(lower, filepath.Join(bundle, "upper"), filepath.Join(bundle, "work"), merged); err != nil {
		return fmt.Errorf("overlay: %w", err)
	}

	workspace, err := r.workspaceDir(c)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(workspace, 0o755); err != nil { //nolint:gosec // shared work dir: the machine's docker daemon and chrooted scripts read it too
		return err
	}
	spec := containerSpec(cfg, c.Env, workspace)
	raw, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(bundle, "config.json"), raw, 0o600); err != nil {
		return err
	}

	logFile, err := os.OpenFile(filepath.Join(bundle, "log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // path under the agent's own data dir
	if err != nil {
		return err
	}
	defer func() { _ = logFile.Close() }()
	r.op(c, "runc", "runc run "+name)
	//nolint:gosec // the runtime's whole job is driving the runc binary
	cmd := exec.CommandContext(ctx, r.runc, "run", "--detach",
		"--pid-file", filepath.Join(bundle, "pid"), name)
	cmd.Dir = bundle
	// runc's stdout/stderr go to the on-disk log, to obs line-by-line (the
	// operator sees the run as if typed by hand), and stderr also to a
	// buffer so the real reason ("container ... already exists", the
	// worker's connection-refused) travels back in the error instead of a
	// bare "exit status 1" that only an ssh into the bundle could explain.
	obsW := &lineWriter{emit: func(l string) { r.op(c, "runc", l) }}
	defer obsW.flush()
	cmd.Stdout = io.MultiWriter(logFile, obsW)
	var errBuf bytes.Buffer
	cmd.Stderr = io.MultiWriter(logFile, &errBuf, obsW)
	if err := cmd.Run(); err != nil {
		if detail := strings.TrimSpace(errBuf.String()); detail != "" {
			return fmt.Errorf("runc run: %w: %s", err, lastLine(detail))
		}
		return fmt.Errorf("runc run: %w", err)
	}
	return nil
}

// lineWriter splits a byte stream into lines and emits each complete one;
// flush releases a trailing partial line. Not concurrency-safe — one
// stream at a time, or guarded by the caller (here runc's stdout and
// stderr share one, and exec.Cmd serialises writes across its pipes).
type lineWriter struct {
	buf  bytes.Buffer
	emit func(string)
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.buf.Write(p)
	for {
		line, err := w.buf.ReadString('\n')
		if err != nil {
			w.buf.Reset()
			w.buf.WriteString(line) // put the partial back
			break
		}
		w.emit(strings.TrimRight(line, "\r\n"))
	}
	return len(p), nil
}

func (w *lineWriter) flush() {
	if rest := strings.TrimSpace(w.buf.String()); rest != "" {
		w.emit(rest)
	}
	w.buf.Reset()
}

// lastLine is the final non-empty line of runc's stderr — the failure
// message, without the whole log's noise.
func lastLine(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

// Stop terminates and removes the container; absence is not an error.
// Every runc call is time-bounded: a stuck container (a D-state process, a
// wedged mount) must not hang the delete forever, because Stop runs on the
// agent's single command goroutine — a hang there zombies the whole agent.
func (r *Runtime) Stop(ctx context.Context, c host.RunContainer) error {
	name := containerName(c)
	sctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if status, err := r.state(sctx, name); err == nil && status == "running" {
		_ = exec.CommandContext(sctx, r.runc, "kill", name, "TERM").Run() //nolint:gosec // see Start
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if s, err := r.state(sctx, name); err != nil || s != "running" {
				break
			}
			select {
			case <-sctx.Done():
				return sctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	_ = exec.CommandContext(sctx, r.runc, "delete", "-f", name).Run() //nolint:gosec // see Start
	bundle := r.bundleDir(c)
	if err := unmountOverlay(filepath.Join(bundle, "merged")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("unmount: %w", err)
	}
	// The workspace dies with the run container — it was never a place
	// to keep things (that is what artifacts are for).
	if workspace, err := r.workspaceDir(c); err == nil {
		_ = os.RemoveAll(workspace)
	}
	return os.RemoveAll(bundle)
}

// Status maps the runc state to the container lifecycle.
func (r *Runtime) Status(ctx context.Context, c host.RunContainer) (host.ContainerStatus, error) {
	state, err := r.state(ctx, containerName(c))
	if err != nil {
		if _, statErr := os.Stat(r.bundleDir(c)); errors.Is(statErr, os.ErrNotExist) {
			return host.StatusStopped, nil
		}
		return host.StatusStopped, nil
	}
	switch state {
	case "running", "created":
		return host.StatusRunning, nil
	default:
		return host.StatusStopped, nil
	}
}

// state asks runc; an unknown container is an error.
func (r *Runtime) state(ctx context.Context, name string) (string, error) {
	out, err := exec.CommandContext(ctx, r.runc, "state", name).Output() //nolint:gosec // see Start
	if err != nil {
		return "", fmt.Errorf("runc state: %w", err)
	}
	var st struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(out, &st); err != nil {
		return "", err
	}
	return st.Status, nil
}

// LogPath names the container's combined output capture (the detached
// runc process inherits it).
func (r *Runtime) LogPath(c host.RunContainer) string {
	return filepath.Join(r.bundleDir(c), "log")
}

func (r *Runtime) bundleDir(c host.RunContainer) string {
	return filepath.Join(r.dataDir, "containers", containerName(c))
}

// workspaceDir is the per-(machine × run) work directory on the
// MACHINE, bind-mounted into the container at the SAME absolute path —
// one path valid everywhere: inside the container, in chrooted machine
// scripts, for the docker daemon's volume binds and build contexts.
func (r *Runtime) workspaceDir(c host.RunContainer) (string, error) {
	abs, err := filepath.Abs(filepath.Join(r.dataDir, "work", containerName(c)))
	if err != nil {
		return "", err
	}
	// Symlinks would break the same-path property: the daemon and the
	// container must agree on the literal string.
	if resolved, err := filepath.EvalSymlinks(filepath.Dir(abs)); err == nil {
		abs = filepath.Join(resolved, filepath.Base(abs))
	}
	return abs, nil
}

// containerName flattens the (machine × run) pair into one runc-safe id.
func containerName(c host.RunContainer) string {
	return sanitizeName(string(c.AgentId)) + "--" + sanitizeName(string(c.RunId))
}

func envList(imageEnv []string, env map[string]string) []string {
	out := make([]string, 0, len(imageEnv)+len(env)+1)
	hasPath := false
	for _, e := range imageEnv {
		if strings.HasPrefix(e, "PATH=") {
			hasPath = true
		}
		out = append(out, e)
	}
	if !hasPath {
		out = append(out, "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	}
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	return out
}
