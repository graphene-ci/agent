package output

import (
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"syscall"
	"time"

	agentpb "github.com/graphene-ci/graphenepb/v1/agent"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Sender interface {
	Output(context.Context, *agentpb.ConnectRequest) error
}

type Streamer struct {
	id           string
	policy       Policy
	budget       *Budget
	sender       Sender
	drainTimeout time.Duration
}

type frame struct {
	stream        agentpb.OutputStream
	data          []byte
	sequence      uint64
	droppedBefore uint64
	observedAt    time.Time
}

type counters struct {
	observed uint64
	sent     uint64
	dropped  uint64
}

type queue struct {
	mu           sync.Mutex
	frames       []frame
	pendingBytes uint64
	inflight     uint64
	sentTotal    uint64
	tailLoss     uint64
	closed       bool
	notify       chan struct{}
	stats        map[agentpb.OutputStream]*counters
	policy       Policy
	budget       *Budget
}

func NewStreamer(id string, policy Policy, budget *Budget, sender Sender, drainTimeout time.Duration) *Streamer {
	return &Streamer{id: id, policy: policy, budget: budget, sender: sender, drainTimeout: drainTimeout}
}

func (s *Streamer) Run(readers map[agentpb.OutputStream]io.Reader) ([]*agentpb.OutputStats, error) {
	q := newQueue(s.policy, s.budget)
	readEvents := make(chan readEvent)
	var readersWait sync.WaitGroup
	for stream, reader := range readers {
		readersWait.Add(1)
		go func(stream agentpb.OutputStream, reader io.Reader) {
			defer readersWait.Done()
			readStream(stream, reader, readEvents)
		}(stream, reader)
	}
	go func() {
		readersWait.Wait()
		close(readEvents)
	}()

	deliveryCtx, cancelDelivery := context.WithCancel(context.Background())
	deliveryDone := make(chan struct{})
	go func() {
		defer close(deliveryDone)
		s.deliver(deliveryCtx, q)
	}()

	partials := make(map[agentpb.OutputStream][]byte)
	ticker := time.NewTicker(s.policy.FlushInterval)
	defer ticker.Stop()
	var firstErr error
	sequence := uint64(0)
	flush := func(stream agentpb.OutputStream) {
		data := partials[stream]
		if len(data) == 0 {
			return
		}
		sequence++
		q.push(frame{stream: stream, data: data, sequence: sequence, observedAt: time.Now()})
		partials[stream] = nil
	}

	for readEvents != nil {
		select {
		case event, ok := <-readEvents:
			if !ok {
				readEvents = nil
				break
			}
			if event.err != nil && firstErr == nil {
				firstErr = event.err
			}
			if len(event.data) == 0 {
				continue
			}
			q.observe(event.stream, uint64(len(event.data)))
			partials[event.stream] = append(partials[event.stream], event.data...)
			for len(partials[event.stream]) >= s.policy.MaxFrameBytes {
				data := make([]byte, s.policy.MaxFrameBytes)
				copy(data, partials[event.stream][:s.policy.MaxFrameBytes])
				partials[event.stream] = partials[event.stream][s.policy.MaxFrameBytes:]
				sequence++
				q.push(frame{stream: event.stream, data: data, sequence: sequence, observedAt: time.Now()})
			}
		case <-ticker.C:
			for stream := range partials {
				flush(stream)
			}
		}
	}
	for stream := range partials {
		flush(stream)
	}
	q.close()

	timer := time.NewTimer(s.drainTimeout)
	select {
	case <-deliveryDone:
		if !timer.Stop() {
			<-timer.C
		}
	case <-timer.C:
		cancelDelivery()
		<-deliveryDone
		q.dropRemaining()
	}
	cancelDelivery()
	return q.outputStats(), firstErr
}

func (s *Streamer) deliver(ctx context.Context, q *queue) {
	for {
		item, ok := q.take(ctx)
		if !ok {
			return
		}
		request := &agentpb.ConnectRequest{
			InstructionId: &agentpb.InstructionId{Value: s.id},
			Event: &agentpb.ConnectRequest_CommandOutput{CommandOutput: &agentpb.CommandOutput{
				Stream:        item.stream,
				Data:          item.data,
				Sequence:      item.sequence,
				DroppedBefore: item.droppedBefore,
				ObservedAt:    timestamppb.New(item.observedAt),
			}},
		}
		err := s.sender.Output(ctx, request)
		q.finish(item, !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded))
		if ctx.Err() != nil {
			return
		}
	}
}

type readEvent struct {
	stream agentpb.OutputStream
	data   []byte
	err    error
}

func readStream(stream agentpb.OutputStream, reader io.Reader, output chan<- readEvent) {
	buffer := make([]byte, 32<<10)
	for {
		count, err := reader.Read(buffer)
		if count > 0 {
			data := make([]byte, count)
			copy(data, buffer[:count])
			output <- readEvent{stream: stream, data: data}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, syscall.EIO) {
				output <- readEvent{stream: stream, err: err}
			}
			return
		}
	}
}

func newQueue(policy Policy, budget *Budget) *queue {
	return &queue{notify: make(chan struct{}, 1), stats: make(map[agentpb.OutputStream]*counters), policy: policy, budget: budget}
}

func (q *queue) observe(stream agentpb.OutputStream, size uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.streamStats(stream).observed += size
}

func (q *queue) push(item frame) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		q.drop(item.stream, uint64(len(item.data)))
		return
	}

	availableInline := q.policy.MaxInlineBytes - min(q.policy.MaxInlineBytes, q.sentTotal+q.pendingBytes)
	suffixLoss := uint64(0)
	if uint64(len(item.data)) > availableInline {
		suffixLoss = uint64(len(item.data)) - availableInline
		item.data = item.data[:availableInline]
		q.drop(item.stream, suffixLoss)
	}
	if len(item.data) == 0 {
		q.tailLoss += suffixLoss
		return
	}

	needed := uint64(len(item.data))
	if q.policy.Overflow == agentpb.OutputOverflowPolicy_OUTPUT_OVERFLOW_POLICY_DROP_OLDEST {
		for q.pendingBytes+needed > q.policy.MaxPendingBytes && len(q.frames) > 0 {
			q.dropFront()
		}
	}
	if q.pendingBytes+needed > q.policy.MaxPendingBytes {
		q.drop(item.stream, needed)
		q.tailLoss += needed + suffixLoss
		return
	}

	acquired := q.budget.TryAcquire(needed)
	if !acquired {
		if q.policy.Overflow == agentpb.OutputOverflowPolicy_OUTPUT_OVERFLOW_POLICY_DROP_OLDEST {
			for len(q.frames) > 0 && !acquired {
				q.dropFront()
				acquired = q.budget.TryAcquire(needed)
			}
			if !acquired {
				q.drop(item.stream, needed)
				q.tailLoss += needed + suffixLoss
				return
			}
		} else {
			q.drop(item.stream, needed)
			q.tailLoss += needed + suffixLoss
			return
		}
	}

	item.droppedBefore = q.tailLoss
	q.tailLoss = 0
	q.frames = append(q.frames, item)
	q.pendingBytes += needed
	q.tailLoss = suffixLoss
	q.signal()
}

func (q *queue) dropFront() {
	item := q.frames[0]
	q.frames = q.frames[1:]
	size := uint64(len(item.data))
	q.pendingBytes -= size
	q.budget.Release(size)
	q.drop(item.stream, size)
	loss := item.droppedBefore + size
	if len(q.frames) > 0 {
		q.frames[0].droppedBefore += loss
	} else {
		q.tailLoss += loss
	}
}

func (q *queue) take(ctx context.Context) (frame, bool) {
	for {
		q.mu.Lock()
		if len(q.frames) > 0 {
			item := q.frames[0]
			q.frames = q.frames[1:]
			q.inflight = uint64(len(item.data))
			q.mu.Unlock()
			return item, true
		}
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return frame{}, false
		}
		select {
		case <-ctx.Done():
			return frame{}, false
		case <-q.notify:
		}
	}
}

func (q *queue) finish(item frame, sent bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	size := uint64(len(item.data))
	q.pendingBytes -= size
	q.inflight = 0
	q.budget.Release(size)
	if sent {
		q.streamStats(item.stream).sent += size
		q.sentTotal += size
	} else {
		q.drop(item.stream, size)
		q.tailLoss += item.droppedBefore + size
	}
	q.signal()
}

func (q *queue) close() {
	q.mu.Lock()
	q.closed = true
	q.signal()
	q.mu.Unlock()
}

func (q *queue) dropRemaining() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.frames) > 0 {
		q.dropFront()
	}
}

func (q *queue) outputStats() []*agentpb.OutputStats {
	q.mu.Lock()
	defer q.mu.Unlock()
	streams := make([]int, 0, len(q.stats))
	for stream := range q.stats {
		streams = append(streams, int(stream))
	}
	sort.Ints(streams)
	result := make([]*agentpb.OutputStats, 0, len(streams))
	for _, raw := range streams {
		stream := agentpb.OutputStream(raw)
		stats := q.stats[stream]
		result = append(result, &agentpb.OutputStats{Stream: stream, ObservedBytes: stats.observed, SentBytes: stats.sent, DroppedBytes: stats.dropped})
	}
	return result
}

func (q *queue) streamStats(stream agentpb.OutputStream) *counters {
	stats := q.stats[stream]
	if stats == nil {
		stats = &counters{}
		q.stats[stream] = stats
	}
	return stats
}

func (q *queue) drop(stream agentpb.OutputStream, size uint64) {
	q.streamStats(stream).dropped += size
}

func (q *queue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}
