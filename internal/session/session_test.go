package session

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/graphene-ci/agent/internal/config"
	"github.com/graphene-ci/agent/pkg/agentpb"
	"github.com/graphene-ci/agent/pkg/host"
	"github.com/graphene-ci/pipeline/pkg/id"
)

// fakeRuntime records calls; Start/Stop succeed instantly.
type fakeRuntime struct {
	mu      sync.Mutex
	pulled  []host.ImageRef
	started []host.RunContainer
	stopped []host.RunContainer
}

func (f *fakeRuntime) Pull(_ context.Context, image host.ImageRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pulled = append(f.pulled, image)
	return nil
}

func (f *fakeRuntime) Start(_ context.Context, c host.RunContainer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, c)
	return nil
}

func (f *fakeRuntime) Stop(_ context.Context, c host.RunContainer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, c)
	return nil
}

func (f *fakeRuntime) Status(_ context.Context, _ host.RunContainer) (host.ContainerStatus, error) {
	return host.StatusRunning, nil
}

// fakeServer implements one Session stream: collects agent messages,
// hands the test a way to push commands.
type fakeServer struct {
	agentpb.UnimplementedAgentAPIServer
	received chan *agentpb.SessionRequest
	commands chan *agentpb.SessionResponse
}

func (s *fakeServer) Session(stream grpc.BidiStreamingServer[agentpb.SessionRequest, agentpb.SessionResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetHello() == nil {
		return errors.New("first message is not hello")
	}
	s.received <- first
	if err := stream.Send(&agentpb.SessionResponse{Body: &agentpb.SessionResponse_HelloAck{
		HelloAck: &agentpb.HelloAck{HeartbeatSeconds: 1},
	}}); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				done <- err
				return
			}
			s.received <- msg
		}
	}()
	for {
		select {
		case cmd := <-s.commands:
			if err := stream.Send(cmd); err != nil {
				return err
			}
		case err := <-done:
			return err
		}
	}
}

func TestSessionLifecycle(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	fake := &fakeServer{
		received: make(chan *agentpb.SessionRequest, 64),
		commands: make(chan *agentpb.SessionResponse, 4),
	}
	agentpb.RegisterAgentAPIServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	store, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rt := &fakeRuntime{}
	cfg := config.Config{
		Server:       lis.Addr().String(),
		Token:        "test-token",
		MachineId:    id.MachineId("vm-1"),
		Insecure:     true,
		ReconnectMin: 10 * time.Millisecond,
		ReconnectMax: 100 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sessionDone := make(chan error, 1)
	go func() {
		sessionDone <- New(cfg, rt, store, "test", slog.New(slog.DiscardHandler)).Run(ctx)
	}()

	// Hello arrives with the machine id and facts.
	hello := recvBody(t, fake.received, func(m *agentpb.SessionRequest) *agentpb.Hello { return m.GetHello() })
	if hello.GetMachineId() != "vm-1" || hello.GetFacts().GetOs() == "" {
		t.Fatalf("hello = %+v", hello)
	}

	// Ensure: the agent pulls, starts, reports, acks.
	fake.commands <- &agentpb.SessionResponse{Body: &agentpb.SessionResponse_EnsureContainer{
		EnsureContainer: &agentpb.EnsureContainer{
			CommandId: "cmd-1",
			Spec: &agentpb.ContainerSpec{
				MachineId: "vm-1",
				RunId:     "run-1",
				Image:     "repo/app:1",
				Env:       map[string]string{"K": "v"},
			},
		},
	}}
	res := recvBody(t, fake.received, func(m *agentpb.SessionRequest) *agentpb.CommandResult { return m.GetCommandResult() })
	if res.GetCommandId() != "cmd-1" || res.GetError() != "" {
		t.Fatalf("ensure result = %+v", res)
	}
	rt.mu.Lock()
	if len(rt.pulled) != 1 || len(rt.started) != 1 || rt.started[0].Env["K"] != "v" {
		t.Fatalf("runtime calls: pulled=%v started=%v", rt.pulled, rt.started)
	}
	rt.mu.Unlock()
	if _, ok := store.Get(id.MachineId("vm-1"), id.RunId("run-1")); !ok {
		t.Fatal("container not recorded in store")
	}

	// Heartbeat carries the container report.
	beat := recvBody(t, fake.received, func(m *agentpb.SessionRequest) *agentpb.Heartbeat { return m.GetHeartbeat() })
	if len(beat.GetContainers()) != 1 || beat.GetContainers()[0].GetState() != agentpb.ContainerState_CONTAINER_STATE_RUNNING {
		t.Fatalf("heartbeat = %+v", beat)
	}

	// Stop: the agent stops and forgets.
	fake.commands <- &agentpb.SessionResponse{Body: &agentpb.SessionResponse_StopContainer{
		StopContainer: &agentpb.StopContainer{CommandId: "cmd-2", MachineId: "vm-1", RunId: "run-1"},
	}}
	res = recvBody(t, fake.received, func(m *agentpb.SessionRequest) *agentpb.CommandResult { return m.GetCommandResult() })
	if res.GetCommandId() != "cmd-2" || res.GetError() != "" {
		t.Fatalf("stop result = %+v", res)
	}
	if _, ok := store.Get(id.MachineId("vm-1"), id.RunId("run-1")); ok {
		t.Fatal("container still in store after stop")
	}

	cancel()
	if err := <-sessionDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
}

// recvBody drains agent messages until extract returns non-nil.
func recvBody[T any, P interface{ *T }](t *testing.T, ch <-chan *agentpb.SessionRequest, extract func(*agentpb.SessionRequest) P) P {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case msg := <-ch:
			if body := extract(msg); body != nil {
				return body
			}
		case <-deadline:
			t.Fatal("timed out waiting for message")
			return nil
		}
	}
}
