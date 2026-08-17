package testserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const testToken = "test-token"

func TestControlStreamReconnectAndConsole(t *testing.T) {
	t.Parallel()
	service, client, stop := testService(t)
	defer stop()
	ctx := authenticatedContext(testToken)
	stream := connectAgent(t, ctx, client, "installation-1")
	if event := waitEvent(t, service.Events()); event.GetHello().GetInstallationId().GetValue() != "installation-1" {
		t.Fatalf("hello = %#v", event)
	}
	output := &bytes.Buffer{}
	console := NewConsole(service, output)
	if err := console.Execute(ctx, `command printf '%s' "hello world"`); err != nil {
		t.Fatal(err)
	}
	instruction, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if got := instruction.GetRunCommand().GetCommand(); got != `printf '%s' "hello world"` {
		t.Fatalf("command = %q", got)
	}
	if instruction.GetId().GetValue() == "" {
		t.Fatal("instruction id is empty")
	}
	if err := service.Disconnect(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatalf("disconnect error = %v", err)
	}
	reconnected := connectAgent(t, ctx, client, "installation-1")
	defer func() { _ = reconnected.CloseSend() }()
	if event := waitEvent(t, service.Events()); event.GetHello() == nil {
		t.Fatalf("reconnect event = %#v", event)
	}
}

func TestArtifactDownloadAndResumableUpload(t *testing.T) {
	t.Parallel()
	service, client, stop := testService(t)
	defer stop()
	ctx := authenticatedContext(testToken)
	control := connectAgent(t, ctx, client, "installation-artifacts")
	defer func() { _ = control.CloseSend() }()
	_ = waitEvent(t, service.Events())

	source := filepath.Join(t.TempDir(), "source.txt")
	if err := os.WriteFile(source, []byte("download-content"), 0o600); err != nil {
		t.Fatal(err)
	}
	metadata, err := service.AddArtifact(source)
	if err != nil {
		t.Fatal(err)
	}
	putID := "put-1"
	if err := service.Submit(ctx, &agentpb.ConnectResponse{
		Id: &agentpb.InstructionId{Value: putID},
		Instruction: &agentpb.ConnectResponse_PutArtifact{PutArtifact: &agentpb.PutArtifact{
			ArtifactId: metadata.GetArtifactId(), Path: "/tmp/result", Size: metadata.GetSize(), Sha256: metadata.GetSha256(),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Recv(); err != nil {
		t.Fatal(err)
	}
	download, err := client.DownloadArtifact(ctx, &agentpb.DownloadArtifactRequest{
		InstructionId: &agentpb.InstructionId{Value: putID}, ArtifactId: metadata.GetArtifactId(), Offset: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	var downloaded []byte
	for {
		frame, recvErr := download.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		downloaded = append(downloaded, frame.GetData()...)
	}
	if string(downloaded) != "nload-content" {
		t.Fatalf("download = %q", downloaded)
	}

	content := []byte("resumable-upload")
	digest := sha256.Sum256(content)
	collectID := "collect-1"
	artifactID := "collected-1"
	if err := service.Submit(ctx, &agentpb.ConnectResponse{
		Id: &agentpb.InstructionId{Value: collectID},
		Instruction: &agentpb.ConnectResponse_CollectArtifact{CollectArtifact: &agentpb.CollectArtifact{
			ArtifactId: &agentpb.ArtifactId{Value: artifactID}, Path: "/tmp/source", Name: "result",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Recv(); err != nil {
		t.Fatal(err)
	}
	first, err := client.UploadArtifact(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Send(uploadBegin(collectID, artifactID, uint64(len(content)), 0)); err != nil {
		t.Fatal(err)
	}
	if err := first.Send(uploadChunk(0, content[:5])); err != nil {
		t.Fatal(err)
	}
	if _, err := first.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("incomplete upload error = %v", err)
	}
	progress, err := client.QueryArtifactUpload(ctx, &agentpb.QueryArtifactUploadRequest{
		InstructionId: &agentpb.InstructionId{Value: collectID}, ArtifactId: &agentpb.ArtifactId{Value: artifactID},
	})
	if err != nil {
		t.Fatal(err)
	}
	prefixDigest := sha256.Sum256(content[:5])
	if progress.GetCommittedSize() != 5 || !bytes.Equal(progress.GetPrefixSha256(), prefixDigest[:]) {
		t.Fatalf("progress = %#v", progress)
	}
	second, err := client.UploadArtifact(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Send(uploadBegin(collectID, artifactID, uint64(len(content)), 5)); err != nil {
		t.Fatal(err)
	}
	if err := second.Send(uploadChunk(5, content[5:])); err != nil {
		t.Fatal(err)
	}
	if err := second.Send(&agentpb.UploadArtifactRequest{Frame: &agentpb.UploadArtifactRequest_Finish{
		Finish: &agentpb.FinishArtifactUpload{Size: uint64(len(content)), Sha256: digest[:]},
	}}); err != nil {
		t.Fatal(err)
	}
	completed, err := second.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if completed.GetArtifactId().GetValue() != artifactID || !bytes.Equal(completed.GetSha256(), digest[:]) {
		t.Fatalf("completed = %#v", completed)
	}
	artifactPath, err := service.ArtifactPath(artifactID)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, content) {
		t.Fatalf("stored = %q", stored)
	}
}

func TestAuthenticationRejectsWrongToken(t *testing.T) {
	t.Parallel()
	_, client, stop := testService(t)
	defer stop()
	_, err := client.QueryArtifactUpload(authenticatedContext("wrong-token"), &agentpb.QueryArtifactUploadRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("error = %v", err)
	}
}

func testService(t *testing.T) (*Server, agentpb.AgentServiceClient, func()) {
	t.Helper()
	service, connection, stop := testTransport(t)
	return service, agentpb.NewAgentServiceClient(connection), stop
}

func testTransport(t *testing.T, options ...grpc.DialOption) (*Server, *grpc.ClientConn, func()) {
	t.Helper()
	service, err := New(t.TempDir(), 4)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(UnaryAuthInterceptor(testToken)),
		grpc.StreamInterceptor(StreamAuthInterceptor(testToken)),
	)
	agentpb.RegisterAgentServiceServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	}
	dialOptions = append(dialOptions, options...)
	connection, err := grpc.NewClient("passthrough:///testserver", dialOptions...)
	if err != nil {
		grpcServer.Stop()
		t.Fatal(err)
	}
	stop := func() {
		_ = connection.Close()
		grpcServer.Stop()
		_ = listener.Close()
	}
	return service, connection, stop
}

func authenticatedContext(token string) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)
}

func connectAgent(t *testing.T, ctx context.Context, client agentpb.AgentServiceClient, installationID string) agentpb.AgentService_ConnectClient {
	t.Helper()
	stream, err := client.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&agentpb.ConnectRequest{Event: &agentpb.ConnectRequest_Hello{Hello: &agentpb.Hello{
		InstallationId: &agentpb.InstallationId{Value: installationID}, ProtocolVersion: "1",
	}}}); err != nil {
		t.Fatal(err)
	}
	return stream
}

func waitEvent(t *testing.T, events <-chan *agentpb.ConnectRequest) *agentpb.ConnectRequest {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for testserver event")
		return nil
	}
}

func uploadBegin(instructionID, artifactID string, size, offset uint64) *agentpb.UploadArtifactRequest {
	return &agentpb.UploadArtifactRequest{Frame: &agentpb.UploadArtifactRequest_Begin{Begin: &agentpb.BeginArtifactUpload{
		InstructionId: &agentpb.InstructionId{Value: instructionID},
		ArtifactId:    &agentpb.ArtifactId{Value: artifactID}, Size: size, Offset: offset,
	}}}
}

func uploadChunk(offset uint64, data []byte) *agentpb.UploadArtifactRequest {
	return &agentpb.UploadArtifactRequest{Frame: &agentpb.UploadArtifactRequest_Chunk{Chunk: &agentpb.ArtifactUploadChunk{
		Offset: offset, Data: data,
	}}}
}
