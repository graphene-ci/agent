package testserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultChunkBytes = 1 << 20
	eventBuffer       = 1024
	instructionBuffer = 64
)

// ErrNoAgent means no agent currently has an active Connect stream.
var ErrNoAgent = errors.New("no agent is connected")

type controlSession struct {
	outbound   chan *agentpb.ConnectResponse
	disconnect chan struct{}
	stopOnce   sync.Once
}

func (s *controlSession) stop() {
	s.stopOnce.Do(func() { close(s.disconnect) })
}

// Server is a local development implementation of AgentService.
type Server struct {
	agentpb.UnimplementedAgentServiceServer

	dataDir    string
	chunkBytes int
	events     chan *agentpb.ConnectRequest

	mu               sync.Mutex
	current          *controlSession
	instructions     map[string]*agentpb.ConnectResponse
	putArtifacts     map[string]string
	collectArtifacts map[string]string
	artifactLocks    map[string]*sync.Mutex
}

// New creates a test server rooted at dataDir.
func New(dataDir string, chunkBytes int) (*Server, error) {
	if dataDir == "" {
		return nil, errors.New("testserver data directory is required")
	}
	if !filepath.IsAbs(dataDir) {
		return nil, errors.New("testserver data directory must be absolute")
	}
	if chunkBytes <= 0 {
		chunkBytes = defaultChunkBytes
	}
	for _, directory := range []string{filepath.Join(dataDir, "artifacts"), filepath.Join(dataDir, "staging")} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			return nil, fmt.Errorf("create testserver storage: %w", err)
		}
	}
	return &Server{
		dataDir: dataDir, chunkBytes: chunkBytes, events: make(chan *agentpb.ConnectRequest, eventBuffer),
		instructions: make(map[string]*agentpb.ConnectResponse),
		putArtifacts: make(map[string]string), collectArtifacts: make(map[string]string),
		artifactLocks: make(map[string]*sync.Mutex),
	}, nil
}

// Events returns agent-to-server control events in receive order.
func (s *Server) Events() <-chan *agentpb.ConnectRequest {
	return s.events
}

// Submit sends one instruction to the currently connected agent.
func (s *Server) Submit(ctx context.Context, instruction *agentpb.ConnectResponse) error {
	if instruction == nil || instruction.GetInstruction() == nil {
		return errors.New("instruction payload is required")
	}
	if instruction.GetPing() == nil && instruction.GetId().GetValue() == "" {
		return errors.New("instruction id is required")
	}
	if err := s.rememberInstruction(instruction); err != nil {
		return err
	}

	s.mu.Lock()
	current := s.current
	s.mu.Unlock()
	if current == nil {
		return ErrNoAgent
	}
	select {
	case current.outbound <- instruction:
		return nil
	case <-current.disconnect:
		return ErrNoAgent
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Disconnect closes the active control stream so reconnect can be exercised.
func (s *Server) Disconnect() error {
	s.mu.Lock()
	current := s.current
	if current != nil {
		s.current = nil
	}
	s.mu.Unlock()
	if current == nil {
		return ErrNoAgent
	}
	current.stop()
	return nil
}

// Connect implements the long-lived agent control stream.
func (s *Server) Connect(stream agentpb.AgentService_ConnectServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetHello() == nil || first.GetInstructionId() != nil {
		return status.Error(codes.InvalidArgument, "first Connect event must be Hello without an instruction id")
	}
	current := &controlSession{
		outbound:   make(chan *agentpb.ConnectResponse, instructionBuffer),
		disconnect: make(chan struct{}),
	}
	if err := s.register(current); err != nil {
		return err
	}
	defer s.unregister(current)
	if err := s.emit(stream.Context(), first); err != nil {
		return err
	}

	received := make(chan error, 1)
	go func() { received <- s.receive(stream) }()
	for {
		select {
		case instruction := <-current.outbound:
			if err := stream.Send(instruction); err != nil {
				return err
			}
		case err := <-received:
			return err
		case <-current.disconnect:
			return status.Error(codes.Unavailable, "testserver disconnected the control stream")
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (s *Server) receive(stream agentpb.AgentService_ConnectServer) error {
	for {
		request, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if request.GetHello() != nil {
			return status.Error(codes.InvalidArgument, "Hello is only valid as the first Connect event")
		}
		if err := s.emit(stream.Context(), request); err != nil {
			return err
		}
	}
}

func (s *Server) emit(ctx context.Context, request *agentpb.ConnectRequest) error {
	select {
	case s.events <- request:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) register(current *controlSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current != nil {
		return status.Error(codes.AlreadyExists, "an agent is already connected")
	}
	s.current = current
	return nil
}

func (s *Server) unregister(current *controlSession) {
	s.mu.Lock()
	if s.current == current {
		s.current = nil
	}
	s.mu.Unlock()
	current.stop()
}

func (s *Server) rememberInstruction(instruction *agentpb.ConnectResponse) error {
	id := instruction.GetId().GetValue()
	if id == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if previous, exists := s.instructions[id]; exists && isInitialInstruction(instruction) {
		return fmt.Errorf("instruction %q was already submitted as %T", id, previous.GetInstruction())
	}
	s.instructions[id] = instruction
	if put := instruction.GetPutArtifact(); put != nil {
		artifactID := put.GetArtifactId().GetValue()
		if artifactID == "" {
			return errors.New("PutArtifact artifact id is required")
		}
		s.putArtifacts[id] = artifactID
	}
	if collect := instruction.GetCollectArtifact(); collect != nil {
		artifactID := collect.GetArtifactId().GetValue()
		if artifactID == "" {
			return errors.New("CollectArtifact artifact id is required")
		}
		s.collectArtifacts[id] = artifactID
	}
	return nil
}

func isInitialInstruction(instruction *agentpb.ConnectResponse) bool {
	return instruction.GetRunCommand() != nil || instruction.GetPutArtifact() != nil ||
		instruction.GetCollectArtifact() != nil || instruction.GetReadFacts() != nil
}

func (s *Server) artifactLock(id string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	lock := s.artifactLocks[id]
	if lock == nil {
		lock = &sync.Mutex{}
		s.artifactLocks[id] = lock
	}
	return lock
}
