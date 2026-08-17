package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type artifactServer struct {
	agentpb.UnimplementedAgentServiceServer
	data            []byte
	artifactID      string
	mu              sync.Mutex
	downloadOffset  uint64
	badDownload     bool
	queryPrefixSize uint64
	uploadBegin     *agentpb.BeginArtifactUpload
	uploaded        []byte
}

func (s *artifactServer) DownloadArtifact(request *agentpb.DownloadArtifactRequest, stream agentpb.AgentService_DownloadArtifactServer) error {
	s.mu.Lock()
	s.downloadOffset = request.GetOffset()
	s.mu.Unlock()
	for offset := request.GetOffset(); offset < uint64(len(s.data)); {
		end := min(offset+3, uint64(len(s.data)))
		responseOffset := offset
		if s.badDownload {
			responseOffset++
		}
		if err := stream.Send(&agentpb.DownloadArtifactResponse{Offset: responseOffset, Data: s.data[offset:end]}); err != nil {
			return err
		}
		offset = end
	}
	return nil
}

func (s *artifactServer) QueryArtifactUpload(context.Context, *agentpb.QueryArtifactUploadRequest) (*agentpb.QueryArtifactUploadResponse, error) {
	prefixDigest := sha256.Sum256(s.data[:s.queryPrefixSize])
	return &agentpb.QueryArtifactUploadResponse{CommittedSize: s.queryPrefixSize, PrefixSha256: prefixDigest[:]}, nil
}

func (s *artifactServer) UploadArtifact(stream agentpb.AgentService_UploadArtifactServer) error {
	data := append([]byte(nil), s.data[:s.queryPrefixSize]...)
	for {
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch frame := request.GetFrame().(type) {
		case *agentpb.UploadArtifactRequest_Begin:
			s.mu.Lock()
			s.uploadBegin = frame.Begin
			s.mu.Unlock()
			if frame.Begin.GetRestart() {
				data = nil
			}
		case *agentpb.UploadArtifactRequest_Chunk:
			if frame.Chunk.GetOffset() != uint64(len(data)) {
				return io.ErrUnexpectedEOF
			}
			data = append(data, frame.Chunk.GetData()...)
		case *agentpb.UploadArtifactRequest_Finish:
			digest := sha256.Sum256(data)
			s.mu.Lock()
			s.uploaded = append([]byte(nil), data...)
			s.mu.Unlock()
			return stream.SendAndClose(&agentpb.UploadArtifactResponse{
				ArtifactId: &agentpb.ArtifactId{Value: s.artifactID},
				Size:       uint64(len(data)), Sha256: digest[:],
			})
		}
	}
}

func TestPutResumesPartialAndCommitsAtomically(t *testing.T) {
	t.Parallel()
	data := []byte("hello world")
	digest := sha256.Sum256(data)
	server := &artifactServer{data: data, artifactID: "artifact-1"}
	client, cleanup := artifactClient(t, server)
	defer cleanup()
	directory := t.TempDir()
	destination := filepath.Join(directory, "result.bin")
	partial := temporaryPath(destination, "instruction-1", "artifact-1")
	if err := os.WriteFile(partial, data[:6], 0o600); err != nil {
		t.Fatal(err)
	}
	manager := New(client, 4, 0o640)
	placed, err := manager.Put(context.Background(), "instruction-1", &agentpb.PutArtifact{
		ArtifactId: &agentpb.ArtifactId{Value: "artifact-1"}, Path: destination,
		Size: uint64(len(data)), Sha256: digest[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, data) || placed.GetSize() != uint64(len(data)) {
		t.Fatalf("placed = %#v, data = %q", placed, actual)
	}
	server.mu.Lock()
	offset := server.downloadOffset
	server.mu.Unlock()
	if offset != 6 {
		t.Fatalf("download offset = %d", offset)
	}
}

func TestPutRestartsFromZeroAfterCorruptPartial(t *testing.T) {
	t.Parallel()
	data := []byte("hello world")
	digest := sha256.Sum256(data)
	server := &artifactServer{data: data, artifactID: "artifact-1"}
	client, cleanup := artifactClient(t, server)
	defer cleanup()
	destination := filepath.Join(t.TempDir(), "result.bin")
	partial := temporaryPath(destination, "instruction-1", "artifact-1")
	if err := os.WriteFile(partial, []byte("broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := New(client, 4, 0o640)
	if _, err := manager.Put(context.Background(), "instruction-1", &agentpb.PutArtifact{
		ArtifactId: &agentpb.ArtifactId{Value: "artifact-1"}, Path: destination,
		Size: uint64(len(data)), Sha256: digest[:],
	}); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, data) {
		t.Fatalf("artifact = %q", actual)
	}
	server.mu.Lock()
	offset := server.downloadOffset
	server.mu.Unlock()
	if offset != 0 {
		t.Fatalf("retry download offset = %d", offset)
	}
}

func TestPutRemovesPartialAfterPermanentProtocolFailure(t *testing.T) {
	t.Parallel()
	data := []byte("hello world")
	digest := sha256.Sum256(data)
	server := &artifactServer{data: data, artifactID: "artifact-1", badDownload: true}
	client, cleanup := artifactClient(t, server)
	defer cleanup()
	destination := filepath.Join(t.TempDir(), "result.bin")
	partial := temporaryPath(destination, "instruction-1", "artifact-1")
	manager := New(client, 4, 0o640)
	_, err := manager.Put(context.Background(), "instruction-1", &agentpb.PutArtifact{
		ArtifactId: &agentpb.ArtifactId{Value: "artifact-1"}, Path: destination,
		Size: uint64(len(data)), Sha256: digest[:],
	})
	if err == nil {
		t.Fatal("expected invalid download offset")
	}
	if _, statErr := os.Stat(partial); !os.IsNotExist(statErr) {
		t.Fatalf("partial artifact remains after permanent failure: %v", statErr)
	}
}

func TestCollectResumesFromVerifiedPrefix(t *testing.T) {
	t.Parallel()
	data := []byte("hello world")
	server := &artifactServer{data: data, artifactID: "artifact-1", queryPrefixSize: 6}
	client, cleanup := artifactClient(t, server)
	defer cleanup()
	path := filepath.Join(t.TempDir(), "source.bin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	manager := New(client, 3, 0o600)
	response, err := manager.Collect(context.Background(), "instruction-1", &agentpb.CollectArtifact{
		ArtifactId: &agentpb.ArtifactId{Value: "artifact-1"}, Path: path, Name: "result",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetSize() != uint64(len(data)) {
		t.Fatalf("response = %#v", response)
	}
	server.mu.Lock()
	begin := server.uploadBegin
	uploaded := append([]byte(nil), server.uploaded...)
	server.mu.Unlock()
	if begin.GetOffset() != 6 || begin.GetRestart() {
		t.Fatalf("begin = %#v", begin)
	}
	if !bytes.Equal(uploaded, data) {
		t.Fatalf("uploaded = %q", uploaded)
	}
}

func artifactClient(t *testing.T, implementation agentpb.AgentServiceServer) (agentpb.AgentServiceClient, func()) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	agentpb.RegisterAgentServiceServer(server, implementation)
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient("passthrough:///artifact-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		server.Stop()
		t.Fatal(err)
	}
	return agentpb.NewAgentServiceClient(connection), func() {
		_ = connection.Close()
		server.Stop()
		_ = listener.Close()
	}
}
