package session

import (
	"context"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
)

type event struct {
	ctx     context.Context
	request *agentpb.ConnectRequest
	result  chan error
}

func (e *event) complete(err error) {
	if e.result == nil {
		return
	}
	select {
	case e.result <- err:
	default:
	}
}

type Outbox struct {
	control chan *event
	output  chan *event
}

func NewOutbox() *Outbox {
	return &Outbox{control: make(chan *event, 256), output: make(chan *event)}
}

func (o *Outbox) Control(ctx context.Context, request *agentpb.ConnectRequest) error {
	return o.submit(ctx, o.control, request)
}

func (o *Outbox) Output(ctx context.Context, request *agentpb.ConnectRequest) error {
	return o.submit(ctx, o.output, request)
}

func (o *Outbox) submit(ctx context.Context, target chan<- *event, request *agentpb.ConnectRequest) error {
	item := &event{ctx: ctx, request: request, result: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case target <- item:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-item.result:
		return err
	}
}
