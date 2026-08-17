package session

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type sessionServer struct {
	agentpb.UnimplementedAgentServiceServer
	testing *testing.T
	done    chan struct{}
	once    sync.Once
}

func (s *sessionServer) Connect(stream agentpb.AgentService_ConnectServer) error {
	metadataValues, ok := metadata.FromIncomingContext(stream.Context())
	if !ok || len(metadataValues.Get("authorization")) != 1 || metadataValues.Get("authorization")[0] != "Bearer token" {
		s.testing.Errorf("authorization metadata = %#v", metadataValues.Get("authorization"))
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetHello() == nil || first.GetHello().GetProtocolVersion() != "1" {
		s.testing.Errorf("first request = %#v", first)
	}
	second, err := stream.Recv()
	if err != nil {
		return err
	}
	if second.GetHeartbeat() == nil || len(second.GetHeartbeat().GetActiveInstructionIds()) != 1 {
		s.testing.Errorf("second request = %#v", second)
	}
	if err := stream.Send(&agentpb.ConnectResponse{Instruction: &agentpb.ConnectResponse_Ping{Ping: &agentpb.Ping{}}}); err != nil {
		return err
	}
	third, err := stream.Recv()
	if err != nil {
		return err
	}
	if third.GetPong() == nil || third.GetInstructionId() != nil {
		s.testing.Errorf("third request = %#v", third)
	}
	s.once.Do(func() { close(s.done) })
	<-stream.Context().Done()
	return stream.Context().Err()
}

type sessionHandler struct{}

func (sessionHandler) Handle(context.Context, *agentpb.ConnectResponse) error { return nil }
func (sessionHandler) ActiveInstructionIDs() []*agentpb.InstructionId {
	return []*agentpb.InstructionId{{Value: "active"}}
}

func TestSessionSendsHelloHeartbeatAndPong(t *testing.T) {
	t.Parallel()
	listener := bufconn.Listen(1 << 20)
	serverImplementation := &sessionServer{testing: t, done: make(chan struct{})}
	server := grpc.NewServer()
	agentpb.RegisterAgentServiceServer(server, serverImplementation)
	go func() { _ = server.Serve(listener) }()
	defer func() {
		server.Stop()
		_ = listener.Close()
	}()
	connection, err := grpc.NewClient("passthrough:///session-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(bearerCredentials{token: "token", allowInsecure: true}),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	control := New(agentpb.NewAgentServiceClient(connection), NewOutbox(), sessionHandler{}, Config{
		Heartbeat: time.Second, ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond,
		Hello: &agentpb.Hello{InstallationId: &agentpb.InstallationId{Value: "installation"}, ProtocolVersion: "1"},
	})
	go func() { runResult <- control.Run(ctx) }()
	select {
	case <-serverImplementation.done:
		cancel()
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("session did not answer Ping")
	}
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}
}

type reconnectClient struct {
	mu           sync.Mutex
	connections  int
	firstReady   chan struct{}
	secondReady  chan struct{}
	secondOutput chan struct{}
}

func (c *reconnectClient) Connect(ctx context.Context, _ ...grpc.CallOption) (grpc.BidiStreamingClient[agentpb.ConnectRequest, agentpb.ConnectResponse], error) {
	c.mu.Lock()
	c.connections++
	connection := c.connections
	c.mu.Unlock()
	return &reconnectStream{
		ctx:       ctx,
		fail:      connection == 1,
		ready:     map[bool]chan struct{}{true: c.firstReady, false: c.secondReady}[connection == 1],
		delivered: c.secondOutput,
	}, nil
}

func (*reconnectClient) DownloadArtifact(context.Context, *agentpb.DownloadArtifactRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[agentpb.DownloadArtifactResponse], error) {
	return nil, errors.New("unexpected DownloadArtifact")
}

func (*reconnectClient) QueryArtifactUpload(context.Context, *agentpb.QueryArtifactUploadRequest, ...grpc.CallOption) (*agentpb.QueryArtifactUploadResponse, error) {
	return nil, errors.New("unexpected QueryArtifactUpload")
}

func (*reconnectClient) UploadArtifact(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[agentpb.UploadArtifactRequest, agentpb.UploadArtifactResponse], error) {
	return nil, errors.New("unexpected UploadArtifact")
}

type reconnectStream struct {
	ctx       context.Context
	fail      bool
	ready     chan struct{}
	delivered chan struct{}
	sends     int
	once      sync.Once
}

func (s *reconnectStream) Send(request *agentpb.ConnectRequest) error {
	s.sends++
	if s.sends == 2 {
		close(s.ready)
	}
	if request.GetCommandOutput() == nil {
		return nil
	}
	if s.fail {
		return errors.New("simulated output transport failure")
	}
	s.once.Do(func() { close(s.delivered) })
	return nil
}

func (s *reconnectStream) Recv() (*agentpb.ConnectResponse, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (*reconnectStream) Header() (metadata.MD, error) { return nil, nil }
func (*reconnectStream) Trailer() metadata.MD         { return nil }
func (*reconnectStream) CloseSend() error             { return nil }
func (s *reconnectStream) Context() context.Context   { return s.ctx }
func (*reconnectStream) SendMsg(any) error            { return errors.New("unexpected SendMsg") }
func (*reconnectStream) RecvMsg(any) error            { return errors.New("unexpected RecvMsg") }

func TestSessionDropsFailedOutputFrameAndContinuesAfterReconnect(t *testing.T) {
	t.Parallel()
	client := &reconnectClient{
		firstReady: make(chan struct{}), secondReady: make(chan struct{}), secondOutput: make(chan struct{}),
	}
	outbox := NewOutbox()
	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	control := New(client, outbox, sessionHandler{}, Config{
		Heartbeat: time.Hour, ReconnectMin: time.Millisecond, ReconnectMax: 5 * time.Millisecond,
		Hello: &agentpb.Hello{InstallationId: &agentpb.InstallationId{Value: "installation"}, ProtocolVersion: "1"},
	})
	go func() { runResult <- control.Run(ctx) }()

	waitFor(t, client.firstReady, "first connection")
	outputContext, stopOutput := context.WithTimeout(context.Background(), time.Second)
	defer stopOutput()
	if err := outbox.Output(outputContext, commandOutput(1)); err != nil {
		cancel()
		t.Fatalf("first output: %v", err)
	}
	waitFor(t, client.secondReady, "reconnected session")
	if err := outbox.Output(outputContext, commandOutput(2)); err != nil {
		cancel()
		t.Fatalf("second output: %v", err)
	}
	waitFor(t, client.secondOutput, "output after reconnect")
	cancel()
	if err := <-runResult; err != nil {
		t.Fatal(err)
	}
}

func commandOutput(sequence uint64) *agentpb.ConnectRequest {
	return &agentpb.ConnectRequest{
		InstructionId: &agentpb.InstructionId{Value: "command"},
		Event: &agentpb.ConnectRequest_CommandOutput{CommandOutput: &agentpb.CommandOutput{
			Stream: agentpb.OutputStream_OUTPUT_STREAM_STDOUT, Data: []byte("data"), Sequence: sequence,
		}},
	}
}

func waitFor(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
