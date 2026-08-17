package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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
	queryPrefixHash []byte
	queryComplete   bool
	queryArtifact   *agentpb.UploadArtifactResponse
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
	prefixHash := prefixDigest[:]
	if s.queryPrefixHash != nil {
		prefixHash = s.queryPrefixHash
	}
	return &agentpb.QueryArtifactUploadResponse{
		CommittedSize: s.queryPrefixSize, PrefixSha256: prefixHash,
		Complete: s.queryComplete, Artifact: s.queryArtifact,
	}, nil
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

func TestCollectRestartsAfterPrefixMismatch(t *testing.T) {
	t.Parallel()
	data := []byte("hello world")
	server := &artifactServer{data: data, artifactID: "artifact-1", queryPrefixSize: 6, queryPrefixHash: make([]byte, sha256.Size)}
	client, cleanup := artifactClient(t, server)
	defer cleanup()
	path := filepath.Join(t.TempDir(), "source.bin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	response, err := New(client, 3, 0o600).Collect(context.Background(), "instruction-1", &agentpb.CollectArtifact{
		ArtifactId: &agentpb.ArtifactId{Value: "artifact-1"}, Path: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	begin := server.uploadBegin
	uploaded := append([]byte(nil), server.uploaded...)
	server.mu.Unlock()
	if !begin.GetRestart() || begin.GetOffset() != 0 || !bytes.Equal(uploaded, data) || response.GetSize() != uint64(len(data)) {
		t.Fatalf("begin = %#v, uploaded = %q, response = %#v", begin, uploaded, response)
	}
}

func TestCollectAcceptsAlreadyCompleteUpload(t *testing.T) {
	t.Parallel()
	data := []byte("complete")
	digest := sha256.Sum256(data)
	metadata := &agentpb.UploadArtifactResponse{ArtifactId: &agentpb.ArtifactId{Value: "artifact-1"}, Size: uint64(len(data)), Sha256: digest[:]}
	server := &artifactServer{data: data, artifactID: "artifact-1", queryComplete: true, queryArtifact: metadata}
	client, cleanup := artifactClient(t, server)
	defer cleanup()
	path := filepath.Join(t.TempDir(), "source.bin")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	response, err := New(client, 3, 0o600).Collect(context.Background(), "instruction-1", &agentpb.CollectArtifact{
		ArtifactId: &agentpb.ArtifactId{Value: "artifact-1"}, Path: path,
	})
	if err != nil || response.GetArtifactId().GetValue() != "artifact-1" || response.GetSize() != uint64(len(data)) || !bytes.Equal(response.GetSha256(), digest[:]) {
		t.Fatalf("Collect() = %#v, %v", response, err)
	}
	server.mu.Lock()
	begin := server.uploadBegin
	server.mu.Unlock()
	if begin != nil {
		t.Fatalf("unexpected upload = %#v", begin)
	}
}

func TestCollectRejectsInvalidCompleteUpload(t *testing.T) {
	t.Parallel()
	data := []byte("complete")
	for _, metadata := range []*agentpb.UploadArtifactResponse{
		nil,
		{ArtifactId: &agentpb.ArtifactId{Value: "another"}, Size: uint64(len(data)), Sha256: make([]byte, sha256.Size)},
		{ArtifactId: &agentpb.ArtifactId{Value: "artifact-1"}, Size: uint64(len(data)), Sha256: make([]byte, sha256.Size)},
	} {
		metadata := metadata
		t.Run("metadata", func(t *testing.T) {
			t.Parallel()
			server := &artifactServer{data: data, artifactID: "artifact-1", queryComplete: true, queryArtifact: metadata}
			client, cleanup := artifactClient(t, server)
			defer cleanup()
			path := filepath.Join(t.TempDir(), "source.bin")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := New(client, 3, 0o600).Collect(context.Background(), "instruction-1", &agentpb.CollectArtifact{
				ArtifactId: &agentpb.ArtifactId{Value: "artifact-1"}, Path: path,
			}); err == nil {
				t.Fatal("expected verification error")
			}
		})
	}
}

func TestArtifactRequestValidation(t *testing.T) {
	t.Parallel()
	manager := New(nil, 3, 0o600)
	validDigest := make([]byte, sha256.Size)
	for _, request := range []*agentpb.PutArtifact{
		nil,
		{},
		{ArtifactId: &agentpb.ArtifactId{Value: "artifact"}, Path: "relative", Sha256: validDigest},
		{ArtifactId: &agentpb.ArtifactId{Value: "artifact"}, Path: "/tmp/file", Sha256: []byte("short")},
		{ArtifactId: &agentpb.ArtifactId{Value: "artifact"}, Path: "/tmp/file", Sha256: validDigest, Mode: 0o10000},
	} {
		if _, err := manager.Put(context.Background(), "instruction", request); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("Put(%#v) error = %v", request, err)
		}
	}
	for _, request := range []*agentpb.CollectArtifact{
		nil,
		{},
		{ArtifactId: &agentpb.ArtifactId{Value: "artifact"}, Path: "relative"},
	} {
		if _, err := manager.Collect(context.Background(), "instruction", request); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("Collect(%#v) error = %v", request, err)
		}
	}
}

func TestCollectRejectsDirectory(t *testing.T) {
	t.Parallel()
	manager := New(nil, 3, 0o600)
	_, err := manager.Collect(context.Background(), "instruction", &agentpb.CollectArtifact{
		ArtifactId: &agentpb.ArtifactId{Value: "artifact"}, Path: t.TempDir(),
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Collect() error = %v", err)
	}
}

func TestCommitFileOverwriteAndConflict(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	destination := filepath.Join(directory, "destination")
	if err := os.WriteFile(destination, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(directory, "source")
	if err := os.WriteFile(source, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := commitFile(source, destination, false); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("commit without overwrite error = %v", err)
	}
	if err := commitFile(source, destination, true); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(destination)
	if err != nil || string(data) != "new" {
		t.Fatalf("destination = %q, %v", data, err)
	}
}

func TestTransient(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		err  error
		want bool
	}{
		{err: status.Error(codes.Unavailable, "retry"), want: true},
		{err: status.Error(codes.DeadlineExceeded, "retry"), want: true},
		{err: status.Error(codes.ResourceExhausted, "retry"), want: true},
		{err: status.Error(codes.Aborted, "retry"), want: true},
		{err: status.Error(codes.InvalidArgument, "stop"), want: false},
	} {
		if got := transient(test.err); got != test.want {
			t.Fatalf("transient(%v) = %t", test.err, got)
		}
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
