// Package session maintains the agent's single outbound stream to the
// server: hello with machine facts, heartbeats with container reports,
// and execution of the server's container commands against the runtime.
package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
}

// New assembles a session.
func New(cfg config.Config, rt host.Runtime, store *Store, version string, log *slog.Logger) *Session {
	return &Session{cfg: cfg, runtime: rt, store: store, version: version, log: log}
}

// Run keeps the agent connected until ctx ends: dial, serve one stream,
// reconnect with backoff. This is the composition point of the session —
// every goroutine of the agent starts under it.
func (s *Session) Run(ctx context.Context) error {
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
	stream, err := agentpb.NewAgentAPIClient(conn).Session(ctx)
	if err != nil {
		return err
	}
	return s.serve(ctx, stream)
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
	heartbeatEvery := time.Duration(ack.GetHeartbeatSeconds()) * time.Second
	if heartbeatEvery == 0 {
		heartbeatEvery = 15 * time.Second
	}
	s.log.Info("connected", "server", s.cfg.Server, "heartbeat", heartbeatEvery)

	group, gctx := errgroup.WithContext(ctx)
	commands := make(chan *agentpb.SessionResponse)
	outbox := make(chan *agentpb.SessionRequest, 16)

	// Receive: the stream is the only input.
	group.Go(func() error {
		defer close(commands)
		for {
			msg, err := stream.Recv()
			if err != nil {
				return fmt.Errorf("recv: %w", err)
			}
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

// execute performs one server command and returns the messages to send.
func (s *Session) execute(ctx context.Context, cmd *agentpb.SessionResponse) []*agentpb.SessionRequest {
	switch body := cmd.GetBody().(type) {
	case *agentpb.SessionResponse_EnsureContainer:
		return s.ensure(ctx, body.EnsureContainer)
	case *agentpb.SessionResponse_StopContainer:
		return s.stop(ctx, body.StopContainer)
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
	if err := s.runtime.Pull(ctx, c.Image); err != nil {
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
