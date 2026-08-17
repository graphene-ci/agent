package output

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
)

type captureSender struct {
	mu       sync.Mutex
	requests []*agentpb.ConnectRequest
}

func (s *captureSender) Output(_ context.Context, request *agentpb.ConnectRequest) error {
	s.mu.Lock()
	s.requests = append(s.requests, request)
	s.mu.Unlock()
	return nil
}

func TestStreamerFramesAndCountsOutput(t *testing.T) {
	t.Parallel()
	sender := &captureSender{}
	policy := Policy{MaxFrameBytes: 3, FlushInterval: time.Millisecond, MaxPendingBytes: 64, MaxInlineBytes: 64, Overflow: agentpb.OutputOverflowPolicy_OUTPUT_OVERFLOW_POLICY_DROP_OLDEST}
	streamer := NewStreamer("id", policy, NewBudget(64), sender, time.Second)
	stats, err := streamer.Run(map[agentpb.OutputStream]io.Reader{
		agentpb.OutputStream_OUTPUT_STREAM_STDOUT: strings.NewReader("abcdefg"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sender.requests) != 3 {
		t.Fatalf("frames = %d", len(sender.requests))
	}
	for index, request := range sender.requests {
		if request.GetCommandOutput().GetSequence() != uint64(index+1) {
			t.Fatalf("sequence[%d] = %d", index, request.GetCommandOutput().GetSequence())
		}
	}
	if len(stats) != 1 || stats[0].GetObservedBytes() != 7 || stats[0].GetSentBytes() != 7 || stats[0].GetDroppedBytes() != 0 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestQueueDropsOldestWithMarker(t *testing.T) {
	t.Parallel()
	budget := NewBudget(4)
	q := newQueue(Policy{MaxPendingBytes: 4, MaxInlineBytes: 32, Overflow: agentpb.OutputOverflowPolicy_OUTPUT_OVERFLOW_POLICY_DROP_OLDEST}, budget)
	q.observe(agentpb.OutputStream_OUTPUT_STREAM_STDOUT, 8)
	q.push(frame{stream: agentpb.OutputStream_OUTPUT_STREAM_STDOUT, data: []byte("old!"), sequence: 1})
	q.push(frame{stream: agentpb.OutputStream_OUTPUT_STREAM_STDOUT, data: []byte("new!"), sequence: 2})
	item, ok := q.take(context.Background())
	if !ok {
		t.Fatal("queue is empty")
	}
	if item.sequence != 2 || item.droppedBefore != 4 {
		t.Fatalf("frame = %#v", item)
	}
	q.finish(item, true)
	stats := q.outputStats()[0]
	if stats.GetObservedBytes() != 8 || stats.GetSentBytes() != 4 || stats.GetDroppedBytes() != 4 {
		t.Fatalf("stats = %#v", stats)
	}
	if budget.Used() != 0 {
		t.Fatalf("budget used = %d", budget.Used())
	}
}
