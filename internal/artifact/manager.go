package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cenkalti/backoff/v7"
	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidArgument  = errors.New("invalid artifact argument")
	ErrChecksumMismatch = errors.New("artifact checksum mismatch")
	ErrAlreadyExists    = errors.New("artifact already exists")
)

type Manager struct {
	client      agentpb.AgentServiceClient
	chunkBytes  int
	defaultMode uint32
}

func New(client agentpb.AgentServiceClient, chunkBytes int, defaultMode uint32) *Manager {
	return &Manager{client: client, chunkBytes: chunkBytes, defaultMode: defaultMode}
}

func (m *Manager) Put(ctx context.Context, instructionID string, request *agentpb.PutArtifact) (*agentpb.ArtifactPlaced, error) {
	if request == nil || request.GetArtifactId() == nil || request.GetArtifactId().GetValue() == "" {
		return nil, fmt.Errorf("%w: artifact id is required", ErrInvalidArgument)
	}
	if !filepath.IsAbs(request.GetPath()) {
		return nil, fmt.Errorf("%w: destination path must be absolute", ErrInvalidArgument)
	}
	if len(request.GetSha256()) != sha256.Size {
		return nil, fmt.Errorf("%w: SHA-256 must contain 32 bytes", ErrInvalidArgument)
	}
	mode := request.GetMode()
	if mode == 0 {
		mode = m.defaultMode
	}
	if mode > 0o7777 {
		return nil, fmt.Errorf("%w: mode contains non-permission bits", ErrInvalidArgument)
	}

	temporary := temporaryPath(request.GetPath(), instructionID, request.GetArtifactId().GetValue())
	checksumRetried := false
	return backoff.Retry(ctx, func() (*agentpb.ArtifactPlaced, error) {
		placed, err := m.downloadAttempt(ctx, instructionID, request, temporary, mode)
		if err == nil {
			return placed, nil
		}
		if ctx.Err() != nil {
			_ = os.Remove(temporary)
			return nil, err
		}
		if errors.Is(err, ErrChecksumMismatch) && !checksumRetried {
			checksumRetried = true
			return nil, backoff.RetryAfter(0, err)
		}
		if !transient(err) {
			_ = os.Remove(temporary)
			return nil, backoff.Permanent(err)
		}
		return nil, err
	}, backoff.WithBackOff(backoff.NewConstantBackOff(500*time.Millisecond)), backoff.WithMaxElapsedTime(0))
}

func (m *Manager) downloadAttempt(ctx context.Context, instructionID string, request *agentpb.PutArtifact, temporary string, mode uint32) (*agentpb.ArtifactPlaced, error) {
	file, err := openPartial(temporary)
	if err != nil {
		return nil, fmt.Errorf("open partial artifact: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat partial artifact: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: partial artifact is not a regular file", ErrInvalidArgument)
	}
	if uint64(info.Size()) > request.GetSize() {
		if err := file.Truncate(0); err != nil {
			return nil, fmt.Errorf("reset oversized partial artifact: %w", err)
		}
		info, err = file.Stat()
		if err != nil {
			return nil, err
		}
	}

	digest := sha256.New()
	offset := uint64(info.Size())
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := io.CopyN(digest, file, int64(offset)); err != nil {
		return nil, fmt.Errorf("hash partial artifact: %w", err)
	}
	if _, err := file.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	stream, err := m.client.DownloadArtifact(ctx, &agentpb.DownloadArtifactRequest{
		InstructionId: &agentpb.InstructionId{Value: instructionID},
		ArtifactId:    request.GetArtifactId(),
		Offset:        offset,
	})
	if err != nil {
		return nil, err
	}
	current := offset
	for {
		response, receiveErr := stream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			break
		}
		if receiveErr != nil {
			_ = file.Sync()
			return nil, receiveErr
		}
		if response.GetOffset() != current {
			return nil, fmt.Errorf("%w: download offset %d, expected %d", ErrInvalidArgument, response.GetOffset(), current)
		}
		if uint64(len(response.GetData())) > request.GetSize()-current {
			return nil, fmt.Errorf("%w: download exceeds declared size", ErrInvalidArgument)
		}
		if _, err := file.Write(response.GetData()); err != nil {
			return nil, fmt.Errorf("write partial artifact: %w", err)
		}
		if _, err := digest.Write(response.GetData()); err != nil {
			return nil, err
		}
		current += uint64(len(response.GetData()))
	}
	if current != request.GetSize() {
		_ = file.Sync()
		return nil, status.Errorf(codes.Unavailable, "download ended at %d of %d bytes", current, request.GetSize())
	}
	actualDigest := digest.Sum(nil)
	if !bytes.Equal(actualDigest, request.GetSha256()) {
		closed = true
		_ = file.Close()
		_ = os.Remove(temporary)
		return nil, ErrChecksumMismatch
	}
	if err := file.Chmod(os.FileMode(mode)); err != nil {
		return nil, fmt.Errorf("set artifact mode: %w", err)
	}
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("sync artifact: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close artifact: %w", err)
	}
	closed = true
	if err := commitFile(temporary, request.GetPath(), request.GetOverwrite()); err != nil {
		return nil, err
	}
	if err := syncDirectory(filepath.Dir(request.GetPath())); err != nil {
		return nil, err
	}
	return &agentpb.ArtifactPlaced{Path: request.GetPath(), Size: current, Sha256: actualDigest}, nil
}

func (m *Manager) Collect(ctx context.Context, instructionID string, request *agentpb.CollectArtifact) (*agentpb.UploadArtifactResponse, error) {
	if request == nil || request.GetArtifactId() == nil || request.GetArtifactId().GetValue() == "" {
		return nil, fmt.Errorf("%w: artifact id is required", ErrInvalidArgument)
	}
	if !filepath.IsAbs(request.GetPath()) {
		return nil, fmt.Errorf("%w: source path must be absolute", ErrInvalidArgument)
	}
	return backoff.Retry(ctx, func() (*agentpb.UploadArtifactResponse, error) {
		result, err := m.uploadAttempt(ctx, instructionID, request)
		if err == nil {
			return result, nil
		}
		if ctx.Err() != nil {
			return nil, err
		}
		if !transient(err) {
			return nil, backoff.Permanent(err)
		}
		return nil, err
	}, backoff.WithBackOff(backoff.NewConstantBackOff(500*time.Millisecond)), backoff.WithMaxElapsedTime(0))
}

func (m *Manager) uploadAttempt(ctx context.Context, instructionID string, request *agentpb.CollectArtifact) (*agentpb.UploadArtifactResponse, error) {
	file, err := openSource(request.GetPath())
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat source artifact: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: source artifact is not a regular file", ErrInvalidArgument)
	}
	size := uint64(info.Size())

	progress, err := m.client.QueryArtifactUpload(ctx, &agentpb.QueryArtifactUploadRequest{
		InstructionId: &agentpb.InstructionId{Value: instructionID},
		ArtifactId:    request.GetArtifactId(),
	})
	if err != nil {
		return nil, err
	}
	if progress.GetComplete() {
		return verifyComplete(file, size, request.GetArtifactId(), progress.GetArtifact())
	}
	if progress.GetCommittedSize() > size {
		return nil, fmt.Errorf("%w: server prefix exceeds source size", ErrInvalidArgument)
	}
	if progress.GetCommittedSize() > 0 && len(progress.GetPrefixSha256()) != sha256.Size {
		return nil, fmt.Errorf("%w: server prefix SHA-256 must contain 32 bytes", ErrInvalidArgument)
	}

	digest := sha256.New()
	offset := progress.GetCommittedSize()
	restart := false
	if offset > 0 {
		if _, err := io.CopyN(digest, file, int64(offset)); err != nil {
			return nil, fmt.Errorf("hash upload prefix: %w", err)
		}
		if !bytes.Equal(digest.Sum(nil), progress.GetPrefixSha256()) {
			restart = true
			offset = 0
			digest.Reset()
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				return nil, err
			}
		}
	}

	stream, err := m.client.UploadArtifact(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&agentpb.UploadArtifactRequest{Frame: &agentpb.UploadArtifactRequest_Begin{Begin: &agentpb.BeginArtifactUpload{
		InstructionId: &agentpb.InstructionId{Value: instructionID},
		ArtifactId:    request.GetArtifactId(),
		Size:          size,
		Offset:        offset,
		Restart:       restart,
	}}}); err != nil {
		return nil, err
	}
	if _, err := file.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}
	buffer := make([]byte, m.chunkBytes)
	current := offset
	remaining := io.LimitReader(file, int64(size-offset))
	for {
		count, readErr := remaining.Read(buffer)
		if count > 0 {
			data := append([]byte(nil), buffer[:count]...)
			if _, err := digest.Write(data); err != nil {
				return nil, err
			}
			if err := stream.Send(&agentpb.UploadArtifactRequest{Frame: &agentpb.UploadArtifactRequest_Chunk{Chunk: &agentpb.ArtifactUploadChunk{Offset: current, Data: data}}}); err != nil {
				return nil, err
			}
			current += uint64(count)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read source artifact: %w", readErr)
		}
	}
	if current != size {
		return nil, fmt.Errorf("%w: source size changed during upload", ErrInvalidArgument)
	}
	latest, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat source artifact after upload: %w", err)
	}
	if latest.Size() != info.Size() || !latest.ModTime().Equal(info.ModTime()) {
		return nil, fmt.Errorf("%w: source changed during upload", ErrInvalidArgument)
	}
	actualDigest := digest.Sum(nil)
	if err := stream.Send(&agentpb.UploadArtifactRequest{Frame: &agentpb.UploadArtifactRequest_Finish{Finish: &agentpb.FinishArtifactUpload{Size: size, Sha256: actualDigest}}}); err != nil {
		return nil, err
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	if err := verifyResponse(response, request.GetArtifactId(), size, actualDigest); err != nil {
		return nil, err
	}
	return response, nil
}

func verifyComplete(file *os.File, size uint64, id *agentpb.ArtifactId, response *agentpb.UploadArtifactResponse) (*agentpb.UploadArtifactResponse, error) {
	if response == nil {
		return nil, fmt.Errorf("%w: complete upload has no artifact metadata", ErrInvalidArgument)
	}
	digest := sha256.New()
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := io.Copy(digest, file); err != nil {
		return nil, err
	}
	if err := verifyResponse(response, id, size, digest.Sum(nil)); err != nil {
		return nil, err
	}
	return response, nil
}

func verifyResponse(response *agentpb.UploadArtifactResponse, id *agentpb.ArtifactId, size uint64, digest []byte) error {
	if response == nil || response.GetArtifactId() == nil || response.GetArtifactId().GetValue() != id.GetValue() {
		return fmt.Errorf("%w: server returned another artifact id", ErrInvalidArgument)
	}
	if response.GetSize() != size || !bytes.Equal(response.GetSha256(), digest) {
		return ErrChecksumMismatch
	}
	return nil
}

func openPartial(path string) (*os.File, error) {
	descriptor, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(descriptor), path), nil
}

func openSource(path string) (*os.File, error) {
	descriptor, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(descriptor), path), nil
}

func commitFile(source, destination string, overwrite bool) error {
	if overwrite {
		if err := os.Rename(source, destination); err != nil {
			return fmt.Errorf("commit artifact: %w", err)
		}
		return nil
	}
	err := unix.Renameat2(unix.AT_FDCWD, source, unix.AT_FDCWD, destination, unix.RENAME_NOREPLACE)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EEXIST) {
		return ErrAlreadyExists
	}
	if !errors.Is(err, unix.ENOSYS) && !errors.Is(err, unix.EINVAL) {
		return fmt.Errorf("commit artifact: %w", err)
	}
	if err := os.Link(source, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrAlreadyExists
		}
		return fmt.Errorf("commit artifact: %w", err)
	}
	if err := os.Remove(source); err != nil {
		return fmt.Errorf("remove committed partial artifact: %w", err)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open artifact directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync artifact directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close artifact directory: %w", err)
	}
	return nil
}

func temporaryPath(destination, instructionID, artifactID string) string {
	digest := sha256.Sum256([]byte(instructionID + "\x00" + artifactID))
	suffix := hex.EncodeToString(digest[:8])
	return filepath.Join(filepath.Dir(destination), "."+filepath.Base(destination)+".graphene-"+suffix+".part")
}

func transient(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	default:
		return false
	}
}
