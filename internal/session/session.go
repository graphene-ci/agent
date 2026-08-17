package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cenkalti/backoff/v7"
	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler interface {
	Handle(context.Context, *agentpb.ConnectResponse) error
	ActiveInstructionIDs() []*agentpb.InstructionId
}

type Config struct {
	Heartbeat    time.Duration
	ReconnectMin time.Duration
	ReconnectMax time.Duration
	Hello        *agentpb.Hello
}

type Session struct {
	client  agentpb.AgentServiceClient
	outbox  *Outbox
	handler Handler
	config  Config
}

type maximumBackOff struct {
	backoff.BackOff
	maximum time.Duration
}

func (b *maximumBackOff) NextBackOff() time.Duration {
	next := b.BackOff.NextBackOff()
	if next == backoff.Stop || next <= b.maximum {
		return next
	}
	return b.maximum
}

type permanentError struct {
	err error
}

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

func New(client agentpb.AgentServiceClient, outbox *Outbox, handler Handler, cfg Config) *Session {
	return &Session{client: client, outbox: outbox, handler: handler, config: cfg}
}

func (s *Session) Run(ctx context.Context) error {
	var pending *event
	retryPolicy := backoff.NewExponentialBackOff()
	retryPolicy.InitialInterval = s.config.ReconnectMin
	retryPolicy.MaxInterval = s.config.ReconnectMax
	retryPolicy.Multiplier = 2
	retryPolicy.RandomizationFactor = 0.5
	retryBackOff := &maximumBackOff{BackOff: retryPolicy, maximum: s.config.ReconnectMax}

	_, err := backoff.Retry(ctx, func() (struct{}, error) {
		nextPending, connected, err := s.runConnection(ctx, pending)
		pending = nextPending
		if err == nil && ctx.Err() != nil {
			return struct{}{}, nil
		}
		if permanent(err) {
			return struct{}{}, backoff.Permanent(err)
		}
		if connected {
			retryBackOff.Reset()
		}
		return struct{}{}, err
	}, backoff.WithBackOff(retryBackOff), backoff.WithMaxElapsedTime(0))

	if ctx.Err() != nil {
		if pending != nil {
			pending.complete(ctx.Err())
		}
		return nil
	}
	if retryErr := backoff.AsRetryError(err); retryErr != nil && errors.Is(retryErr.Cause, backoff.ErrPermanent) {
		return retryErr.LastErr
	}
	return err
}

func (s *Session) runConnection(ctx context.Context, pending *event) (*event, bool, error) {
	connectionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := s.client.Connect(connectionCtx)
	if err != nil {
		return pending, false, fmt.Errorf("open Connect: %w", err)
	}
	if err := stream.Send(&agentpb.ConnectRequest{Event: &agentpb.ConnectRequest_Hello{Hello: s.config.Hello}}); err != nil {
		return pending, false, fmt.Errorf("send Hello: %w", err)
	}
	if err := stream.Send(s.heartbeat()); err != nil {
		return pending, false, fmt.Errorf("send initial Heartbeat: %w", err)
	}

	received := make(chan receiveResult, 1)
	go receive(stream, received)
	ticker := time.NewTicker(s.config.Heartbeat)
	defer ticker.Stop()

	for {
		if pending != nil {
			if err := pending.ctx.Err(); err != nil {
				pending.complete(err)
				pending = nil
				continue
			}
			if err := stream.Send(pending.request); err != nil {
				return pending, true, fmt.Errorf("send control event: %w", err)
			}
			pending.complete(nil)
			pending = nil
			continue
		}

		select {
		case pending = <-s.outbox.control:
			continue
		default:
		}

		select {
		case <-ctx.Done():
			return pending, true, ctx.Err()
		case pending = <-s.outbox.control:
		case item := <-s.outbox.output:
			if err := item.ctx.Err(); err != nil {
				item.complete(err)
				continue
			}
			err := stream.Send(item.request)
			// Output is deliberately not replayed: after an ambiguous transport
			// failure the producer advances its sequence and later frames continue
			// on the next Connect stream. Control events use the pending retry path.
			item.complete(nil)
			if err != nil {
				return pending, true, fmt.Errorf("send command output: %w", err)
			}
		case result := <-received:
			if result.err != nil {
				return pending, true, fmt.Errorf("receive Connect response: %w", result.err)
			}
			if result.response.GetPing() != nil {
				if result.response.GetId() != nil {
					return pending, true, permanentError{err: errors.New("ping must not contain an instruction id")}
				}
				pending = &event{ctx: ctx, request: &agentpb.ConnectRequest{Event: &agentpb.ConnectRequest_Pong{Pong: &agentpb.Pong{}}}}
			} else if err := s.handler.Handle(ctx, result.response); err != nil {
				return pending, true, permanentError{err: fmt.Errorf("handle Connect response: %w", err)}
			}
			go receive(stream, received)
		case <-ticker.C:
			if err := stream.Send(s.heartbeat()); err != nil {
				return pending, true, fmt.Errorf("send Heartbeat: %w", err)
			}
		}
	}
}

func (s *Session) heartbeat() *agentpb.ConnectRequest {
	return &agentpb.ConnectRequest{Event: &agentpb.ConnectRequest_Heartbeat{Heartbeat: &agentpb.Heartbeat{ActiveInstructionIds: s.handler.ActiveInstructionIDs()}}}
}

type receiveResult struct {
	response *agentpb.ConnectResponse
	err      error
}

func receive(stream agentpb.AgentService_ConnectClient, target chan<- receiveResult) {
	response, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		err = status.Error(codes.Unavailable, "Connect closed by server")
	}
	target <- receiveResult{response: response, err: err}
}

func permanent(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var local permanentError
	if errors.As(err, &local) {
		return true
	}
	switch status.Code(err) {
	case codes.Unauthenticated, codes.PermissionDenied, codes.FailedPrecondition, codes.InvalidArgument, codes.Unimplemented:
		return true
	default:
		return false
	}
}
