package testserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/google/uuid"
	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var artifactIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// AddArtifact imports a local file as an immutable downloadable artifact.
func (s *Server) AddArtifact(source string) (*agentpb.UploadArtifactResponse, error) {
	id := uuid.NewString()
	destination, err := s.artifactPath(id)
	if err != nil {
		return nil, err
	}
	input, err := os.Open(source)
	if err != nil {
		return nil, fmt.Errorf("open artifact source: %w", err)
	}
	defer func() { _ = input.Close() }()
	temporary, err := os.CreateTemp(filepath.Dir(destination), ".import-*")
	if err != nil {
		return nil, fmt.Errorf("create artifact temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	hash := sha256.New()
	size, err := io.Copy(io.MultiWriter(temporary, hash), input)
	if err != nil {
		return nil, fmt.Errorf("copy artifact: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return nil, fmt.Errorf("synchronize artifact: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return nil, fmt.Errorf("close artifact: %w", err)
	}
	if err := os.Rename(temporaryPath, destination); err != nil {
		return nil, fmt.Errorf("commit artifact: %w", err)
	}
	committed = true
	return artifactMetadata(id, uint64(size), hash.Sum(nil)), nil
}

// ArtifactPath returns the path of a committed testserver artifact.
func (s *Server) ArtifactPath(id string) (string, error) {
	path, err := s.artifactPath(id)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(path); err != nil {
		return "", err
	}
	return path, nil
}

// DownloadArtifact streams an authorized committed artifact from offset.
func (s *Server) DownloadArtifact(request *agentpb.DownloadArtifactRequest, stream agentpb.AgentService_DownloadArtifactServer) error {
	instructionID := request.GetInstructionId().GetValue()
	artifactID := request.GetArtifactId().GetValue()
	if !s.authorizedPut(instructionID, artifactID) {
		return status.Error(codes.PermissionDenied, "artifact is not authorized by the PutArtifact instruction")
	}
	path, err := s.artifactPath(artifactID)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return status.Error(codes.NotFound, "artifact does not exist")
	}
	if err != nil {
		return status.Errorf(codes.Internal, "open artifact: %v", err)
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return status.Errorf(codes.Internal, "inspect artifact: %v", err)
	}
	if request.GetOffset() > uint64(info.Size()) {
		return status.Error(codes.OutOfRange, "download offset exceeds artifact size")
	}
	if _, err := file.Seek(int64(request.GetOffset()), io.SeekStart); err != nil {
		return status.Errorf(codes.Internal, "seek artifact: %v", err)
	}
	buffer := make([]byte, s.chunkBytes)
	offset := request.GetOffset()
	for {
		count, readErr := file.Read(buffer)
		if count > 0 {
			data := append([]byte(nil), buffer[:count]...)
			if err := stream.Send(&agentpb.DownloadArtifactResponse{Offset: offset, Data: data}); err != nil {
				return err
			}
			offset += uint64(count)
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return status.Errorf(codes.Internal, "read artifact: %v", readErr)
		}
	}
}

// QueryArtifactUpload reports committed or staged progress for an authorized upload.
func (s *Server) QueryArtifactUpload(_ context.Context, request *agentpb.QueryArtifactUploadRequest) (*agentpb.QueryArtifactUploadResponse, error) {
	instructionID := request.GetInstructionId().GetValue()
	artifactID := request.GetArtifactId().GetValue()
	if !s.authorizedCollect(instructionID, artifactID) {
		return nil, status.Error(codes.PermissionDenied, "artifact is not authorized by the CollectArtifact instruction")
	}
	lock := s.artifactLock(artifactID)
	lock.Lock()
	defer lock.Unlock()
	if path, err := s.artifactPath(artifactID); err == nil {
		if size, digest, hashErr := hashFile(path); hashErr == nil {
			metadata := artifactMetadata(artifactID, size, digest)
			return &agentpb.QueryArtifactUploadResponse{
				CommittedSize: size, Complete: true, PrefixSha256: digest, Artifact: metadata,
			}, nil
		} else if !errors.Is(hashErr, os.ErrNotExist) {
			return nil, status.Errorf(codes.Internal, "read committed artifact: %v", hashErr)
		}
	}
	staging, err := s.stagingPath(artifactID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	size, digest, err := hashFile(staging)
	if errors.Is(err, os.ErrNotExist) {
		return &agentpb.QueryArtifactUploadResponse{}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read staged artifact: %v", err)
	}
	return &agentpb.QueryArtifactUploadResponse{CommittedSize: size, PrefixSha256: digest}, nil
}

// UploadArtifact accepts a resumable authorized artifact upload.
func (s *Server) UploadArtifact(stream agentpb.AgentService_UploadArtifactServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	begin := first.GetBegin()
	if begin == nil {
		return status.Error(codes.InvalidArgument, "first upload frame must be begin")
	}
	instructionID := begin.GetInstructionId().GetValue()
	artifactID := begin.GetArtifactId().GetValue()
	if !s.authorizedCollect(instructionID, artifactID) {
		return status.Error(codes.PermissionDenied, "artifact is not authorized by the CollectArtifact instruction")
	}
	if begin.GetOffset() > begin.GetSize() {
		return status.Error(codes.OutOfRange, "upload offset exceeds declared size")
	}
	lock := s.artifactLock(artifactID)
	lock.Lock()
	defer lock.Unlock()

	staging, err := s.stagingPath(artifactID)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	file, err := openStaging(staging, begin.GetOffset(), begin.GetRestart())
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "open staged upload: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	offset := begin.GetOffset()
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			_ = file.Sync()
			return status.Error(codes.InvalidArgument, "upload ended before finish")
		}
		if recvErr != nil {
			_ = file.Sync()
			return recvErr
		}
		if chunk := frame.GetChunk(); chunk != nil {
			if chunk.GetOffset() != offset {
				return status.Errorf(codes.InvalidArgument, "chunk offset %d does not match expected %d", chunk.GetOffset(), offset)
			}
			if uint64(len(chunk.GetData())) > begin.GetSize()-offset {
				return status.Error(codes.OutOfRange, "upload chunk exceeds declared size")
			}
			if _, err := file.Write(chunk.GetData()); err != nil {
				return status.Errorf(codes.Internal, "write staged upload: %v", err)
			}
			offset += uint64(len(chunk.GetData()))
			if err := file.Sync(); err != nil {
				return status.Errorf(codes.Internal, "synchronize staged upload: %v", err)
			}
			continue
		}
		finish := frame.GetFinish()
		if finish == nil {
			return status.Error(codes.InvalidArgument, "upload frame must be chunk or finish")
		}
		if finish.GetSize() != begin.GetSize() || offset != begin.GetSize() {
			return status.Error(codes.InvalidArgument, "finished upload size does not match begin or received bytes")
		}
		if err := file.Sync(); err != nil {
			return status.Errorf(codes.Internal, "synchronize completed upload: %v", err)
		}
		if err := file.Close(); err != nil {
			return status.Errorf(codes.Internal, "close completed upload: %v", err)
		}
		closed = true
		size, digest, err := hashFile(staging)
		if err != nil {
			return status.Errorf(codes.Internal, "verify completed upload: %v", err)
		}
		if size != finish.GetSize() || !bytes.Equal(digest, finish.GetSha256()) {
			return status.Error(codes.DataLoss, "completed upload digest does not match finish")
		}
		destination, err := s.artifactPath(artifactID)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		if _, err := os.Stat(destination); err == nil {
			return status.Error(codes.AlreadyExists, "immutable artifact already exists")
		} else if !errors.Is(err, os.ErrNotExist) {
			return status.Errorf(codes.Internal, "inspect artifact destination: %v", err)
		}
		if err := os.Rename(staging, destination); err != nil {
			return status.Errorf(codes.Internal, "commit uploaded artifact: %v", err)
		}
		return stream.SendAndClose(artifactMetadata(artifactID, size, digest))
	}
}

func openStaging(path string, offset uint64, restart bool) (*os.File, error) {
	if restart {
		if offset != 0 {
			return nil, errors.New("restart requires offset zero")
		}
		return os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if uint64(info.Size()) != offset {
		_ = file.Close()
		return nil, fmt.Errorf("staged size %d does not match offset %d", info.Size(), offset)
	}
	if _, err := file.Seek(int64(offset), io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func (s *Server) authorizedPut(instructionID, artifactID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return instructionID != "" && artifactID != "" && s.putArtifacts[instructionID] == artifactID
}

func (s *Server) authorizedCollect(instructionID, artifactID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return instructionID != "" && artifactID != "" && s.collectArtifacts[instructionID] == artifactID
}

func (s *Server) artifactPath(id string) (string, error) {
	if !artifactIDPattern.MatchString(id) {
		return "", errors.New("invalid artifact id")
	}
	return filepath.Join(s.dataDir, "artifacts", id), nil
}

func (s *Server) stagingPath(id string) (string, error) {
	if !artifactIDPattern.MatchString(id) {
		return "", errors.New("invalid artifact id")
	}
	return filepath.Join(s.dataDir, "staging", id+".part"), nil
}

func hashFile(path string) (uint64, []byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return 0, nil, err
	}
	return uint64(size), hash.Sum(nil), nil
}

func artifactMetadata(id string, size uint64, digest []byte) *agentpb.UploadArtifactResponse {
	return &agentpb.UploadArtifactResponse{
		ArtifactId: &agentpb.ArtifactId{Value: id}, Size: size, Sha256: append([]byte(nil), digest...),
	}
}
