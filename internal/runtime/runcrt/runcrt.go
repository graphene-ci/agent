// Package runcrt is the production Runtime: it hosts a container by
// pulling the OCI image through the server's registry proxy, unpacking a
// flattened rootfs, and running it with runc — no docker, no daemon. The
// container shares the host network (user code exists to change the
// machine and must reach the server); pid/mount/ipc/uts namespaces are
// its own. Requires root and a runc binary — both are the agent
// installer's job.
package runcrt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	insecure bool // host[:port] of the server's registry proxy
	token    string // scoped token: registry proxy auth
	runc     string // runc binary path
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
	}
}

// Start brings the container up: overlay-mounts the pulled image rootfs,
// writes the OCI spec, and runs it detached. Starting a running
// container is a no-op.
func (r *Runtime) Start(ctx context.Context, c host.RunContainer) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if status, err := r.Status(ctx, c); err == nil && status == host.StatusRunning {
		return nil
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
	//nolint:gosec // the runtime's whole job is driving the runc binary
	cmd := exec.CommandContext(ctx, r.runc, "run", "--detach",
		"--pid-file", filepath.Join(bundle, "pid"), containerName(c))
	cmd.Dir = bundle
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("runc run: %w", err)
	}
	return nil
}

// Stop terminates and removes the container; absence is not an error.
func (r *Runtime) Stop(ctx context.Context, c host.RunContainer) error {
	name := containerName(c)
	if status, err := r.state(ctx, name); err == nil && status == "running" {
		_ = exec.CommandContext(ctx, r.runc, "kill", name, "TERM").Run() //nolint:gosec // see Start
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if s, err := r.state(ctx, name); err != nil || s != "running" {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	_ = exec.CommandContext(ctx, r.runc, "delete", "-f", name).Run() //nolint:gosec // see Start
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
