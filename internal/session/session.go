// Package session maintains the agent's single outbound stream to the
// server: hello with machine facts, heartbeats with container reports,
// and execution of the server's container commands against the runtime.
package session

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/graphene-ci/agent/internal/config"
	"github.com/graphene-ci/agent/internal/facts"
	"github.com/graphene-ci/agent/pkg/host"
	agentpb "github.com/graphene-ci/agent/pkg/proto/agent/v1"
	"github.com/graphene-ci/pipeline/pkg/id"
)

// Session is the agent's connection to the server, alive across
// reconnects for the life of Run.
type Session struct {
	cfg     config.Config
	runtime host.Runtime
	store   *Store
	version string
	log     *slog.Logger
	tail    *tailers
	// obs ships the agent's OWN log to the telemetry plane; nil disables
	// it (tests). Fed the live conn alongside the tailer.
	obs *obsShip
	// runCtx bounds container tailers to the agent's life, not one
	// stream's — containers survive reconnects.
	runCtx context.Context //nolint:containedctx // set once by Run, the composition point
}

// New assembles a session. ship carries the agent's own log to obs; pass
// nil to disable (the tailer and stream work without it).
func New(cfg config.Config, rt host.Runtime, store *Store, version string, log *slog.Logger, ship *obsShip) *Session {
	return &Session{cfg: cfg, runtime: rt, store: store, version: version, log: log, tail: newTailers(log), obs: ship}
}

// Run keeps the agent connected until ctx ends: dial, serve one stream,
// reconnect with backoff. This is the composition point of the session —
// every goroutine of the agent starts under it.
func (s *Session) Run(ctx context.Context) error {
	s.runCtx = ctx
	if s.obs != nil {
		go s.obs.runFlusher(ctx)
	}
	backoff := s.cfg.ReconnectMin
	for {
		err := s.connectAndServe(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.log.Error("session ended, reconnecting", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, s.cfg.ReconnectMax)
	}
}

func (s *Session) connectAndServe(ctx context.Context) error {
	conn, err := Dial(s.cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	// Closing the conn is what unblocks a Recv that a wedged transport
	// would otherwise hold: on SIGTERM (ctx done) the agent must exit
	// promptly, not sit in systemd's stop timeout until SIGKILL. Bounded
	// to this connection so a normal stream-end does not leak the waiter.
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-connCtx.Done()
		_ = conn.Close()
	}()
	s.tail.setConn(conn)
	defer s.tail.setConn(nil)
	if s.obs != nil {
		s.obs.setConn(conn)
		defer s.obs.setConn(nil)
	}
	stream, err := agentpb.NewAgentAPIClient(conn).Session(ctx)
	if err != nil {
		return err
	}
	return s.serve(ctx, stream)
}

// selfUpdate replaces the running binary when the server's differs.
// The downloaded binary's digest must match the announced one before it
// replaces ours — a corrupt or wrong download never overwrites a working
// agent. On success the process exits so systemd (Restart=always) brings
// the new binary up; the caller does not return.
func (s *Session) selfUpdate(ctx context.Context, wantDigest string) error {
	if wantDigest == "" {
		return nil
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	have, err := fileDigest(exe)
	if err != nil {
		return err
	}
	if have == wantDigest {
		return nil
	}
	s.log.Info("agent binary out of date, self-updating", "have", short(have), "want", short(wantDigest))
	scheme := "https"
	if s.cfg.Insecure {
		scheme = "http"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, scheme+"://"+s.cfg.Server+"/agent/binary", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download: %s", resp.Status)
	}
	tmp := exe + ".new"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755) //nolint:gosec // the agent binary is executable
	if err != nil {
		return err
	}
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, h), resp.Body); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	_ = f.Close()
	if got := hex.EncodeToString(h.Sum(nil)); got != wantDigest {
		_ = os.Remove(tmp)
		return fmt.Errorf("downloaded digest %s != announced %s", short(got), short(wantDigest))
	}
	if err := os.Rename(tmp, exe); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	s.log.Info("agent binary updated, restarting to run it")
	os.Exit(0)
	return nil
}

// fileDigest is the sha256 of a file, hex-encoded.
func fileDigest(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // our own executable path
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func short(digest string) string {
	if len(digest) > 12 {
		return digest[:12]
	}
	return digest
}

func (s *Session) serve(ctx context.Context, stream agentpb.AgentAPI_SessionClient) error {
	hello := &agentpb.SessionRequest{Body: &agentpb.SessionRequest_Hello{Hello: &agentpb.Hello{
		AgentId:      string(s.cfg.AgentId),
		AgentVersion: s.version,
		Facts:        facts.Collect(),
	}}}
	if err := stream.Send(hello); err != nil {
		return fmt.Errorf("send hello: %w", err)
	}
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("await hello ack: %w", err)
	}
	ack := first.GetHelloAck()
	if ack == nil {
		return errors.New("server did not ack hello")
	}
	// Self-update: the server serves the canonical agent binary and its
	// digest. If ours differs, download the new one and re-exec — the
	// binary that installs at boot is otherwise frozen forever (RotateToken
	// renews the credential, never the code).
	if err := s.selfUpdate(ctx, ack.GetAgentBinaryDigest()); err != nil {
		s.log.Warn("self-update skipped", "error", err)
	}
	heartbeatEvery := time.Duration(ack.GetHeartbeatSeconds()) * time.Second
	if heartbeatEvery == 0 {
		heartbeatEvery = 15 * time.Second
	}
	s.log.Info("connected", "server", s.cfg.Server, "heartbeat", heartbeatEvery)

	group, gctx := errgroup.WithContext(ctx)
	commands := make(chan *agentpb.SessionResponse)
	outbox := make(chan *agentpb.SessionRequest, 16)
	// PTY sessions are mortal with THIS stream: opened by its frames,
	// buried when it ends. Their frames bypass the container-command
	// queue — a keystroke must not wait behind an image pull.
	shells := newPtys()
	defer shells.closeAll()

	// Receive: the stream is the only input.
	group.Go(func() error {
		defer close(commands)
		for {
			msg, err := stream.Recv()
			if err != nil {
				return fmt.Errorf("recv: %w", err)
			}
			if shells.handle(gctx, msg, outbox) {
				continue
			}
			// Every server command the agent takes is visible in obs — the
			// receipt itself, before any work, so a command that arrives but
			// never acts is diagnosable without ssh.
			s.log.Info("server command received", "command", commandName(msg))
			select {
			case commands <- msg:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
	})

	// Execute: container commands run one at a time — the agent hosts a
	// handful of containers, ordering beats parallel pulls.
	group.Go(func() error {
		for {
			select {
			case cmd, ok := <-commands:
				if !ok {
					return nil
				}
				for _, msg := range s.execute(gctx, cmd) {
					select {
					case outbox <- msg:
					case <-gctx.Done():
						return gctx.Err()
					}
				}
			case <-gctx.Done():
				return gctx.Err()
			}
		}
	})

	// Send: single writer — gRPC streams allow one concurrent sender.
	group.Go(func() error {
		ticker := time.NewTicker(heartbeatEvery)
		defer ticker.Stop()
		for {
			select {
			case msg := <-outbox:
				if err := stream.Send(msg); err != nil {
					return fmt.Errorf("send: %w", err)
				}
			case <-ticker.C:
				beat := &agentpb.SessionRequest{Body: &agentpb.SessionRequest_Heartbeat{
					Heartbeat: &agentpb.Heartbeat{Containers: s.reports(gctx)},
				}}
				if err := stream.Send(beat); err != nil {
					return fmt.Errorf("send heartbeat: %w", err)
				}
			case <-gctx.Done():
				return gctx.Err()
			}
		}
	})

	return group.Wait()
}

// commandName is the wire kind of a server command, for the obs log of
// its receipt.
func commandName(msg *agentpb.SessionResponse) string {
	switch msg.GetBody().(type) {
	case *agentpb.SessionResponse_EnsureContainer:
		return "ensure-container"
	case *agentpb.SessionResponse_StopContainer:
		return "stop-container"
	case *agentpb.SessionResponse_RotateToken:
		return "rotate-token"
	default:
		return "unknown"
	}
}

// execute performs one server command and returns the messages to send.
func (s *Session) execute(ctx context.Context, cmd *agentpb.SessionResponse) []*agentpb.SessionRequest {
	switch body := cmd.GetBody().(type) {
	case *agentpb.SessionResponse_EnsureContainer:
		return s.ensure(ctx, body.EnsureContainer)
	case *agentpb.SessionResponse_StopContainer:
		return s.stop(ctx, body.StopContainer)
	case *agentpb.SessionResponse_RotateToken:
		s.rotate(body.RotateToken.GetToken())
		return nil
	default:
		s.log.Warn("unknown server message ignored")
		return nil
	}
}

func (s *Session) ensure(ctx context.Context, cmd *agentpb.EnsureContainer) []*agentpb.SessionRequest {
	spec := cmd.GetSpec()
	c, err := containerFromSpec(spec)
	if err != nil {
		return []*agentpb.SessionRequest{result(cmd.GetCommandId(), err)}
	}
	if err := s.store.Put(c); err != nil {
		return []*agentpb.SessionRequest{result(cmd.GetCommandId(), err)}
	}
	var out []*agentpb.SessionRequest
	out = append(out, report(c, agentpb.ContainerState_CONTAINER_STATE_PULLING, ""))
	if err := s.runtime.Pull(ctx, c); err != nil {
		s.log.Error("pull failed", "image", c.Image, "error", err)
		return append(out,
			report(c, agentpb.ContainerState_CONTAINER_STATE_FAILED, err.Error()),
			result(cmd.GetCommandId(), err))
	}
	if err := s.runtime.Start(ctx, c); err != nil {
		s.log.Error("start failed", "machine", c.AgentId, "run", c.RunId, "error", err)
		return append(out,
			report(c, agentpb.ContainerState_CONTAINER_STATE_FAILED, err.Error()),
			result(cmd.GetCommandId(), err))
	}
	s.log.Info("container running", "machine", c.AgentId, "run", c.RunId, "image", c.Image)
	// From here the container's stdout is telemetry: tail the capture
	// into the door for the life of the container.
	s.tail.start(s.runCtx, c, s.runtime.LogPath(c))
	return append(out,
		report(c, agentpb.ContainerState_CONTAINER_STATE_RUNNING, ""),
		result(cmd.GetCommandId(), nil))
}

func (s *Session) stop(ctx context.Context, cmd *agentpb.StopContainer) []*agentpb.SessionRequest {
	agentId, err := id.ParseAgentId(cmd.GetAgentId())
	if err != nil {
		return []*agentpb.SessionRequest{result(cmd.GetCommandId(), err)}
	}
	runId, err := id.ParseRunId(cmd.GetRunId())
	if err != nil {
		return []*agentpb.SessionRequest{result(cmd.GetCommandId(), err)}
	}
	c, ok := s.store.Get(agentId, runId)
	if !ok {
		// Unknown to the store — still ask the runtime, idempotently.
		c = host.RunContainer{AgentId: agentId, RunId: runId}
	}
	s.tail.stop(c)
	if err := s.runtime.Stop(ctx, c); err != nil {
		return []*agentpb.SessionRequest{result(cmd.GetCommandId(), err)}
	}
	if err := s.store.Delete(c); err != nil {
		return []*agentpb.SessionRequest{result(cmd.GetCommandId(), err)}
	}
	s.log.Info("container stopped", "machine", c.AgentId, "run", c.RunId)
	return []*agentpb.SessionRequest{
		report(c, agentpb.ContainerState_CONTAINER_STATE_STOPPED, ""),
		result(cmd.GetCommandId(), nil),
	}
}

// reports snapshots every known container's state for a heartbeat.
func (s *Session) reports(ctx context.Context) []*agentpb.ContainerReport {
	containers := s.store.List()
	out := make([]*agentpb.ContainerReport, 0, len(containers))
	for _, c := range containers {
		var state agentpb.ContainerState
		var message string
		status, err := s.runtime.Status(ctx, c)
		if err != nil {
			state = agentpb.ContainerState_CONTAINER_STATE_FAILED
			message = err.Error()
		} else {
			state = toProtoState(status)
		}
		out = append(out, &agentpb.ContainerReport{
			AgentId: string(c.AgentId),
			RunId:   string(c.RunId),
			State:   state,
			Message: message,
		})
	}
	return out
}

func containerFromSpec(spec *agentpb.ContainerSpec) (host.RunContainer, error) {
	var c host.RunContainer
	agentId, err := id.ParseAgentId(spec.GetAgentId())
	if err != nil {
		return c, err
	}
	runId, err := id.ParseRunId(spec.GetRunId())
	if err != nil {
		return c, err
	}
	c = host.RunContainer{
		AgentId: agentId,
		RunId:   runId,
		Image:   host.ImageRef(spec.GetImage()),
		Env:     spec.GetEnv(),
	}
	return c, c.Validate()
}

func toProtoState(s host.ContainerStatus) agentpb.ContainerState {
	switch s {
	case host.StatusPulling:
		return agentpb.ContainerState_CONTAINER_STATE_PULLING
	case host.StatusRunning:
		return agentpb.ContainerState_CONTAINER_STATE_RUNNING
	case host.StatusStopped:
		return agentpb.ContainerState_CONTAINER_STATE_STOPPED
	case host.StatusFailed:
		return agentpb.ContainerState_CONTAINER_STATE_FAILED
	default:
		return agentpb.ContainerState_CONTAINER_STATE_UNSPECIFIED
	}
}

func report(c host.RunContainer, state agentpb.ContainerState, message string) *agentpb.SessionRequest {
	return &agentpb.SessionRequest{Body: &agentpb.SessionRequest_ContainerReport{
		ContainerReport: &agentpb.ContainerReport{
			AgentId: string(c.AgentId),
			RunId:   string(c.RunId),
			State:   state,
			Message: message,
		},
	}}
}

func result(commandId string, err error) *agentpb.SessionRequest {
	msg := &agentpb.CommandResult{CommandId: commandId}
	if err != nil {
		msg.Error = err.Error()
	}
	return &agentpb.SessionRequest{Body: &agentpb.SessionRequest_CommandResult{CommandResult: msg}}
}

// rotate swaps the agent's credential: the next dial uses the fresh
// token, the running stream stays on the old one, and the env file is
// rewritten so a machine reboot comes back with the fresh one too.
func (s *Session) rotate(token string) {
	if token == "" {
		return
	}
	s.cfg.Token = token
	path := os.Getenv("GRAPHENE_AGENT_ENV_FILE")
	if path == "" {
		path = "/etc/graphene-agent/env"
	}
	raw, err := os.ReadFile(path) //nolint:gosec // the agent's own env file
	if err != nil {
		s.log.Warn("token rotated in memory only: env file unreadable", "path", path, "error", err)
		return
	}
	lines := strings.Split(string(raw), "\n")
	replaced := false
	for i, line := range lines {
		if strings.HasPrefix(line, config.EnvToken+"=") {
			lines[i] = config.EnvToken + "=" + token
			replaced = true
		}
	}
	if !replaced {
		lines = append(lines, config.EnvToken+"="+token)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		s.log.Warn("token rotated in memory only: env file not writable", "path", path, "error", err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		s.log.Warn("token rotated in memory only: env file not replaceable", "path", path, "error", err)
		return
	}
	s.log.Info("agent token rotated")
}
