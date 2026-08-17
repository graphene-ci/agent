package agent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/graphene-ci/agent/internal/artifact"
	"github.com/graphene-ci/agent/internal/command"
	"github.com/graphene-ci/agent/internal/facts"
	"github.com/graphene-ci/agent/internal/output"
	"github.com/graphene-ci/agent/internal/protocol"
	"github.com/graphene-ci/agent/internal/state"
	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Sender interface {
	Control(context.Context, *agentpb.ConnectRequest) error
	Output(context.Context, *agentpb.ConnectRequest) error
}

type Config struct {
	Command       command.Config
	GlobalOutput  uint64
	OutputDrain   time.Duration
	MaxConcurrent int
}

type Agent struct {
	store     *state.Store
	sender    Sender
	facts     *facts.Reader
	artifacts *artifact.Manager
	config    Config
	budget    *output.Budget
	semaphore chan struct{}
	mu        sync.RWMutex
	active    map[string]*operation
}

type operation struct {
	id       string
	kind     string
	cancel   context.CancelFunc
	controls chan commandControl
	mu       sync.RWMutex
	process  *command.Process
}

type commandControl struct {
	input  *agentpb.CommandInput
	resize *agentpb.ResizeTerminal
	signal *agentpb.SignalCommand
}

func New(store *state.Store, sender Sender, factsReader *facts.Reader, artifacts *artifact.Manager, cfg Config) *Agent {
	return &Agent{
		store: store, sender: sender, facts: factsReader, artifacts: artifacts, config: cfg,
		budget: output.NewBudget(cfg.GlobalOutput), semaphore: make(chan struct{}, cfg.MaxConcurrent), active: make(map[string]*operation),
	}
}

func (a *Agent) Handle(ctx context.Context, response *agentpb.ConnectResponse) error {
	if response == nil || response.GetInstruction() == nil {
		return errors.New("ConnectResponse instruction is absent")
	}
	id, err := protocol.InstructionID(response.GetId())
	if err != nil {
		return err
	}

	switch instruction := response.GetInstruction().(type) {
	case *agentpb.ConnectResponse_RunCommand:
		return a.start(ctx, id, "command", func(operationCtx context.Context, operation *operation) {
			a.runCommand(ctx, operationCtx, operation, instruction.RunCommand)
		})
	case *agentpb.ConnectResponse_ReadFacts:
		return a.start(ctx, id, "facts", func(operationCtx context.Context, operation *operation) {
			a.readFacts(ctx, operationCtx, operation, instruction.ReadFacts)
		})
	case *agentpb.ConnectResponse_PutArtifact:
		return a.start(ctx, id, "put_artifact", func(operationCtx context.Context, operation *operation) {
			a.putArtifact(ctx, operationCtx, operation, instruction.PutArtifact)
		})
	case *agentpb.ConnectResponse_CollectArtifact:
		return a.start(ctx, id, "collect_artifact", func(operationCtx context.Context, operation *operation) {
			a.collectArtifact(ctx, operationCtx, operation, instruction.CollectArtifact)
		})
	case *agentpb.ConnectResponse_CommandInput:
		return a.control(id, commandControl{input: instruction.CommandInput})
	case *agentpb.ConnectResponse_ResizeTerminal:
		return a.control(id, commandControl{resize: instruction.ResizeTerminal})
	case *agentpb.ConnectResponse_SignalCommand:
		return a.control(id, commandControl{signal: instruction.SignalCommand})
	case *agentpb.ConnectResponse_CancelOperation:
		return a.cancel(id)
	case *agentpb.ConnectResponse_Ping:
		return errors.New("ping must be handled by the session")
	default:
		return fmt.Errorf("unsupported ConnectResponse instruction %T", instruction)
	}
}

func (a *Agent) ActiveInstructionIDs() []*agentpb.InstructionId {
	a.mu.RLock()
	ids := make([]string, 0, len(a.active))
	for id := range a.active {
		ids = append(ids, id)
	}
	a.mu.RUnlock()
	sort.Strings(ids)
	result := make([]*agentpb.InstructionId, 0, len(ids))
	for _, id := range ids {
		result = append(result, &agentpb.InstructionId{Value: id})
	}
	return result
}

func (a *Agent) start(parent context.Context, id, kind string, run func(context.Context, *operation)) error {
	reserved, err := a.store.Reserve(id, kind)
	if err != nil {
		return err
	}
	if !reserved {
		return nil
	}
	select {
	case a.semaphore <- struct{}{}:
	default:
		go a.fail(parent, id, agentpb.ErrorCode_ERROR_CODE_INTERNAL, "maximum active instruction limit reached")
		return nil
	}

	operationCtx, cancel := context.WithCancel(parent)
	item := &operation{id: id, kind: kind, cancel: cancel, controls: make(chan commandControl, 128)}
	a.mu.Lock()
	a.active[id] = item
	a.mu.Unlock()
	if err := a.store.Activate(id); err != nil {
		a.remove(item)
		return err
	}
	go func() {
		defer a.remove(item)
		run(operationCtx, item)
	}()
	return nil
}

func (a *Agent) remove(operation *operation) {
	operation.cancel()
	a.mu.Lock()
	delete(a.active, operation.id)
	a.mu.Unlock()
	<-a.semaphore
}

func (a *Agent) control(id string, control commandControl) error {
	a.mu.RLock()
	operation := a.active[id]
	a.mu.RUnlock()
	if operation == nil || operation.kind != "command" {
		return fmt.Errorf("command instruction %q is not active", id)
	}
	select {
	case operation.controls <- control:
		return nil
	default:
		return fmt.Errorf("command instruction %q control queue is full", id)
	}
}

func (a *Agent) cancel(id string) error {
	a.mu.RLock()
	operation := a.active[id]
	a.mu.RUnlock()
	if operation == nil {
		return nil
	}
	operation.cancel()
	operation.mu.RLock()
	process := operation.process
	operation.mu.RUnlock()
	if process != nil {
		process.Cancel()
	}
	return nil
}

func (a *Agent) readFacts(parent, operationCtx context.Context, operation *operation, request *agentpb.ReadFacts) {
	results := a.facts.Read(operationCtx, request)
	if operationCtx.Err() != nil {
		a.fail(parent, operation.id, agentpb.ErrorCode_ERROR_CODE_CANCELED, "fact observation canceled")
		return
	}
	event := &agentpb.ConnectRequest{
		InstructionId: &agentpb.InstructionId{Value: operation.id},
		Event:         &agentpb.ConnectRequest_FactsRead{FactsRead: &agentpb.FactsRead{Results: results, ObservedAt: timestamppb.Now()}},
	}
	if err := a.sender.Control(parent, event); err != nil {
		_ = a.store.Complete(operation.id, "facts result delivery failed")
		return
	}
	_ = a.store.Complete(operation.id, "facts_read")
}

func (a *Agent) runCommand(parent, operationCtx context.Context, operation *operation, request *agentpb.RunCommand) {
	policy, err := output.Normalize(request.GetOutput(), a.config.GlobalOutput)
	if err != nil {
		a.fail(parent, operation.id, agentpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, err.Error())
		return
	}
	process, err := command.Start(operationCtx, a.config.Command, request)
	if err != nil {
		a.fail(parent, operation.id, agentpb.ErrorCode_ERROR_CODE_START_FAILED, err.Error())
		return
	}
	operation.mu.Lock()
	operation.process = process
	operation.mu.Unlock()
	defer func() { _ = process.Close() }()

	gate := make(chan struct{})
	streamer := output.NewStreamer(operation.id, policy, a.budget, gatedSender{gate: gate, sender: a.sender}, a.config.OutputDrain)
	statsResult := make(chan outputResult, 1)
	go func() {
		stats, streamErr := streamer.Run(process.Readers())
		statsResult <- outputResult{stats: stats, err: streamErr}
	}()

	started := &agentpb.ConnectRequest{
		InstructionId: &agentpb.InstructionId{Value: operation.id},
		Event:         &agentpb.ConnectRequest_CommandStarted{CommandStarted: &agentpb.CommandStarted{Pid: &agentpb.ProcessId{Value: process.PID()}}},
	}
	startedErr := a.sender.Control(parent, started)
	close(gate)
	if startedErr != nil {
		process.Cancel()
	}

	controlErrors := make(chan error, 1)
	go commandControls(operationCtx, operation, controlErrors)
	processResult := make(chan command.Result, 1)
	go func() { processResult <- process.Wait() }()

	var result command.Result
	var controlErr error
	select {
	case result = <-processResult:
	case controlErr = <-controlErrors:
		process.Cancel()
		result = <-processResult
	}
	streamed := <-statsResult
	if controlErr == nil {
		controlErr = streamed.err
	}
	if startedErr != nil {
		_ = a.store.Complete(operation.id, "CommandStarted delivery failed")
		return
	}
	if controlErr != nil {
		a.fail(parent, operation.id, agentpb.ErrorCode_ERROR_CODE_IO, controlErr.Error())
		return
	}
	if result.Err != nil {
		a.fail(parent, operation.id, agentpb.ErrorCode_ERROR_CODE_IO, result.Err.Error())
		return
	}
	if result.Canceled {
		a.fail(parent, operation.id, agentpb.ErrorCode_ERROR_CODE_CANCELED, "command canceled")
		return
	}
	exited := &agentpb.ConnectRequest{
		InstructionId: &agentpb.InstructionId{Value: operation.id},
		Event: &agentpb.ConnectRequest_CommandExited{CommandExited: &agentpb.CommandExited{
			ExitCode: result.ExitCode, Signal: result.Signal, TimedOut: result.TimedOut, Output: streamed.stats,
		}},
	}
	if err := a.sender.Control(parent, exited); err != nil {
		_ = a.store.Complete(operation.id, "CommandExited delivery failed")
		return
	}
	_ = a.store.Complete(operation.id, "command_exited")
}

type outputResult struct {
	stats []*agentpb.OutputStats
	err   error
}

type gatedSender struct {
	gate   <-chan struct{}
	sender Sender
}

func (g gatedSender) Output(ctx context.Context, request *agentpb.ConnectRequest) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.gate:
		return g.sender.Output(ctx, request)
	}
}

func commandControls(ctx context.Context, operation *operation, failures chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		case control := <-operation.controls:
			operation.mu.RLock()
			process := operation.process
			operation.mu.RUnlock()
			if process == nil {
				failures <- errors.New("command control arrived before process start")
				return
			}
			var err error
			switch {
			case control.input != nil:
				err = process.Input(control.input.GetData(), control.input.GetClose())
			case control.resize != nil:
				err = process.Resize(control.resize.GetSize())
			case control.signal != nil:
				err = process.Signal(control.signal.GetSignal())
			}
			if err != nil {
				failures <- err
				return
			}
		}
	}
}

func (a *Agent) putArtifact(parent, operationCtx context.Context, operation *operation, request *agentpb.PutArtifact) {
	placed, err := a.artifacts.Put(operationCtx, operation.id, request)
	if err != nil {
		a.fail(parent, operation.id, failureCode(err), err.Error())
		return
	}
	event := &agentpb.ConnectRequest{InstructionId: &agentpb.InstructionId{Value: operation.id}, Event: &agentpb.ConnectRequest_ArtifactPlaced{ArtifactPlaced: placed}}
	if err := a.sender.Control(parent, event); err != nil {
		_ = a.store.Complete(operation.id, "ArtifactPlaced delivery failed")
		return
	}
	_ = a.store.Complete(operation.id, "artifact_placed")
}

func (a *Agent) collectArtifact(parent, operationCtx context.Context, operation *operation, request *agentpb.CollectArtifact) {
	_, err := a.artifacts.Collect(operationCtx, operation.id, request)
	if err != nil {
		a.fail(parent, operation.id, failureCode(err), err.Error())
		return
	}
	_ = a.store.Complete(operation.id, "artifact_uploaded")
}

func (a *Agent) fail(parent context.Context, id string, code agentpb.ErrorCode, message string) {
	_ = a.sender.Control(parent, protocol.Failure(id, code, message))
	_ = a.store.Complete(id, "operation_failed")
}

func failureCode(err error) agentpb.ErrorCode {
	switch {
	case errors.Is(err, artifact.ErrInvalidArgument):
		return agentpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT
	case errors.Is(err, artifact.ErrChecksumMismatch):
		return agentpb.ErrorCode_ERROR_CODE_CHECKSUM_MISMATCH
	case errors.Is(err, artifact.ErrAlreadyExists):
		return agentpb.ErrorCode_ERROR_CODE_ALREADY_EXISTS
	default:
		return protocol.ErrorCode(err)
	}
}
