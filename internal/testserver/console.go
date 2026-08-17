package testserver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"github.com/mattn/go-shellwords"
	"google.golang.org/protobuf/encoding/protojson"
)

// ErrQuit means the console received the quit command.
var ErrQuit = errors.New("quit testserver")

// Console translates interactive commands into agent instructions.
type Console struct {
	server *Server
	output io.Writer
	mu     sync.Mutex
}

// NewConsole creates a console that writes JSON lines to output.
func NewConsole(server *Server, output io.Writer) *Console {
	return &Console{server: server, output: output}
}

// Run consumes commands until EOF, cancellation, or quit.
func (c *Console) Run(ctx context.Context, input io.Reader) error {
	go c.writeEvents(ctx)
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if err := c.Execute(ctx, line); err != nil {
			if errors.Is(err, ErrQuit) {
				return nil
			}
			c.write(map[string]any{"error": err.Error()})
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read console: %w", err)
	}
	return nil
}

// Execute parses and submits one console command.
func (c *Console) Execute(ctx context.Context, line string) error {
	command, remainder, _ := strings.Cut(strings.TrimSpace(line), " ")
	switch command {
	case "command", "terminal":
		if strings.TrimSpace(remainder) == "" {
			return fmt.Errorf("usage: %s <shell text>", command)
		}
		id := uuid.NewString()
		request := &agentpb.ConnectResponse{
			Id: &agentpb.InstructionId{Value: id},
			Instruction: &agentpb.ConnectResponse_RunCommand{RunCommand: &agentpb.RunCommand{
				Command: remainder, Terminal: command == "terminal",
			}},
		}
		return c.submit(ctx, command, id, request, nil)
	case "facts":
		arguments, err := shellwords.Parse(remainder)
		if err != nil {
			return err
		}
		if len(arguments) > 1 || len(arguments) == 1 && arguments[0] != "sensitive" {
			return errors.New("usage: facts [sensitive]")
		}
		id := uuid.NewString()
		request := &agentpb.ConnectResponse{
			Id: &agentpb.InstructionId{Value: id},
			Instruction: &agentpb.ConnectResponse_ReadFacts{ReadFacts: &agentpb.ReadFacts{
				IncludeSensitive: len(arguments) == 1,
			}},
		}
		return c.submit(ctx, command, id, request, nil)
	case "put":
		arguments, err := shellwords.Parse(remainder)
		if err != nil || len(arguments) != 2 {
			return errors.New("usage: put <testserver-source> <agent-destination>")
		}
		artifact, err := c.server.AddArtifact(arguments[0])
		if err != nil {
			return err
		}
		id := uuid.NewString()
		request := &agentpb.ConnectResponse{
			Id: &agentpb.InstructionId{Value: id},
			Instruction: &agentpb.ConnectResponse_PutArtifact{PutArtifact: &agentpb.PutArtifact{
				ArtifactId: artifact.GetArtifactId(), Path: arguments[1], Overwrite: true,
				Size: artifact.GetSize(), Sha256: artifact.GetSha256(),
			}},
		}
		return c.submit(ctx, command, id, request, map[string]any{"artifact_id": artifact.GetArtifactId().GetValue()})
	case "collect":
		arguments, err := shellwords.Parse(remainder)
		if err != nil || len(arguments) < 1 || len(arguments) > 2 {
			return errors.New("usage: collect <agent-source> [artifact-name]")
		}
		name := filepath.Base(arguments[0])
		if len(arguments) == 2 {
			name = arguments[1]
		}
		id := uuid.NewString()
		artifactID := uuid.NewString()
		request := &agentpb.ConnectResponse{
			Id: &agentpb.InstructionId{Value: id},
			Instruction: &agentpb.ConnectResponse_CollectArtifact{CollectArtifact: &agentpb.CollectArtifact{
				ArtifactId: &agentpb.ArtifactId{Value: artifactID}, Path: arguments[0], Name: name,
			}},
		}
		return c.submit(ctx, command, id, request, map[string]any{
			"artifact_id": artifactID, "artifact_path": filepath.Join(c.server.dataDir, "artifacts", artifactID),
		})
	case "input":
		arguments, err := shellwords.Parse(remainder)
		if err != nil || len(arguments) < 2 {
			return errors.New("usage: input <instruction-id> <text>")
		}
		id := arguments[0]
		request := &agentpb.ConnectResponse{
			Id: &agentpb.InstructionId{Value: id},
			Instruction: &agentpb.ConnectResponse_CommandInput{CommandInput: &agentpb.CommandInput{
				Data: []byte(strings.Join(arguments[1:], " ")),
			}},
		}
		return c.submit(ctx, command, id, request, nil)
	case "close-input":
		arguments, err := shellwords.Parse(remainder)
		if err != nil || len(arguments) != 1 {
			return errors.New("usage: close-input <instruction-id>")
		}
		id := arguments[0]
		request := &agentpb.ConnectResponse{
			Id:          &agentpb.InstructionId{Value: id},
			Instruction: &agentpb.ConnectResponse_CommandInput{CommandInput: &agentpb.CommandInput{Close: true}},
		}
		return c.submit(ctx, command, id, request, nil)
	case "resize":
		arguments, err := shellwords.Parse(remainder)
		if err != nil || len(arguments) != 3 {
			return errors.New("usage: resize <instruction-id> <columns> <rows>")
		}
		columns, err := parseUint32(arguments[1])
		if err != nil {
			return fmt.Errorf("columns: %w", err)
		}
		rows, err := parseUint32(arguments[2])
		if err != nil {
			return fmt.Errorf("rows: %w", err)
		}
		id := arguments[0]
		request := &agentpb.ConnectResponse{
			Id: &agentpb.InstructionId{Value: id},
			Instruction: &agentpb.ConnectResponse_ResizeTerminal{ResizeTerminal: &agentpb.ResizeTerminal{
				Size: &agentpb.TerminalSize{Columns: columns, Rows: rows},
			}},
		}
		return c.submit(ctx, command, id, request, nil)
	case "signal":
		arguments, err := shellwords.Parse(remainder)
		if err != nil || len(arguments) != 2 {
			return errors.New("usage: signal <instruction-id> interrupt|terminate|kill")
		}
		signal, err := parseSignal(arguments[1])
		if err != nil {
			return err
		}
		id := arguments[0]
		request := &agentpb.ConnectResponse{
			Id:          &agentpb.InstructionId{Value: id},
			Instruction: &agentpb.ConnectResponse_SignalCommand{SignalCommand: &agentpb.SignalCommand{Signal: signal}},
		}
		return c.submit(ctx, command, id, request, nil)
	case "cancel":
		arguments, err := shellwords.Parse(remainder)
		if err != nil || len(arguments) != 1 {
			return errors.New("usage: cancel <instruction-id>")
		}
		id := arguments[0]
		request := &agentpb.ConnectResponse{
			Id:          &agentpb.InstructionId{Value: id},
			Instruction: &agentpb.ConnectResponse_CancelOperation{CancelOperation: &agentpb.CancelOperation{}},
		}
		return c.submit(ctx, command, id, request, nil)
	case "ping":
		return c.submit(ctx, command, "", &agentpb.ConnectResponse{
			Instruction: &agentpb.ConnectResponse_Ping{Ping: &agentpb.Ping{}},
		}, nil)
	case "disconnect":
		if err := c.server.Disconnect(); err != nil {
			return err
		}
		c.write(map[string]any{"action": "disconnect", "submitted": true})
		return nil
	case "help":
		c.write(map[string]any{"commands": []string{
			"command <shell text>", "terminal <shell text>", "input <id> <text>", "close-input <id>",
			"resize <id> <columns> <rows>", "signal <id> interrupt|terminate|kill", "cancel <id>",
			"facts [sensitive]", "put <server-source> <agent-destination>",
			"collect <agent-source> [name]", "ping", "disconnect", "quit",
		}})
		return nil
	case "quit", "exit":
		return ErrQuit
	default:
		return fmt.Errorf("unknown command %q; use help", command)
	}
}

func (c *Console) submit(ctx context.Context, action, id string, request *agentpb.ConnectResponse, fields map[string]any) error {
	if err := c.server.Submit(ctx, request); err != nil {
		return err
	}
	message := map[string]any{"action": action, "submitted": true}
	if id != "" {
		message["instruction_id"] = id
	}
	for key, value := range fields {
		message[key] = value
	}
	c.write(message)
	return nil
}

func (c *Console) writeEvents(ctx context.Context) {
	marshal := protojson.MarshalOptions{UseProtoNames: true}
	for {
		select {
		case event := <-c.server.Events():
			encoded, err := marshal.Marshal(event)
			if err != nil {
				c.write(map[string]any{"error": fmt.Sprintf("encode event: %v", err)})
				continue
			}
			c.writeRaw(encoded)
		case <-ctx.Done():
			return
		}
	}
}

func (c *Console) write(value any) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return
	}
	c.writeRaw(encoded)
}

func (c *Console) writeRaw(encoded []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, _ = c.output.Write(append(encoded, '\n'))
}

func parseUint32(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || parsed == 0 {
		return 0, errors.New("value must be a positive 32-bit integer")
	}
	return uint32(parsed), nil
}

func parseSignal(value string) (agentpb.Signal, error) {
	switch value {
	case "interrupt":
		return agentpb.Signal_SIGNAL_INTERRUPT, nil
	case "terminate":
		return agentpb.Signal_SIGNAL_TERMINATE, nil
	case "kill":
		return agentpb.Signal_SIGNAL_KILL, nil
	default:
		return agentpb.Signal_SIGNAL_UNSPECIFIED, errors.New("signal must be interrupt, terminate, or kill")
	}
}
