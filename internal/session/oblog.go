// The agent's OWN operational log — connect, disconnect, self-update,
// session errors — is a dimension-3 signal like any other: it belongs in
// obs under entity agent/<id>, read with `graphenectl logs agent <id>`,
// not behind an ssh + journalctl. obsHandler is a slog.Handler that tees
// every record to the inner handler (stderr → journald, always) and to
// the telemetry plane as an OTLP log of agent/<id>.
//
// The records worth seeing are exactly the ones emitted while the session
// is down — a broken stream, a reconnect, a self-update — so a record
// with no live connection is not dropped: it waits in a bounded ring and
// ships when a connection returns. Best effort: obs never blocks the
// agent, and an overflowing ring drops its OLDEST (a live tail beats a
// stale backlog).
package session

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"

	"github.com/graphene-ci/pipeline/pkg/id"
)

const (
	obLogRingMax    = 512
	obLogFlushEvery = 2 * time.Second
)

// obsShip holds the connection and the pending ring shared by every
// obsHandler derived through WithAttrs/WithGroup.
type obsShip struct {
	agentId string
	mu      sync.Mutex
	conn    *grpc.ClientConn
	ring    []*logspb.LogRecord
}

func NewObsShip(agentId string) *obsShip { return &obsShip{agentId: agentId} }

// setConn points the ship at the current session connection; a fresh
// connection is the moment to flush whatever the disconnect buffered.
func (s *obsShip) setConn(conn *grpc.ClientConn) {
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
}

// Op ships one raw line of a machine operation (image pull, runc) as a
// dimension-3 log of the RUN — so `graphenectl logs run/<id>` shows the
// same output a person would see running it by hand. Attributed by the
// graphene.run record attribute; the batch's resource already carries
// graphene.agent. stream names the operation ("pull", "runc").
func (s *obsShip) Op(agentID id.AgentId, runID id.RunId, stream, line string) {
	s.add(&logspb.LogRecord{
		TimeUnixNano:   uint64(time.Now().UnixNano()), //nolint:gosec // wall clock is positive
		SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
		SeverityText:   "INFO",
		Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: line}},
		Attributes: []*commonpb.KeyValue{
			strAttr("stream", stream),
			strAttr("graphene.run", string(runID)),
		},
	})
}

func (s *obsShip) add(rec *logspb.LogRecord) {
	s.mu.Lock()
	s.ring = append(s.ring, rec)
	if over := len(s.ring) - obLogRingMax; over > 0 {
		s.ring = s.ring[over:] // drop oldest
	}
	s.mu.Unlock()
}

// runFlusher ships the ring on a tick for the life of the agent. One
// goroutine, started from Run — not one per record.
func (s *obsShip) runFlusher(ctx context.Context) {
	ticker := time.NewTicker(obLogFlushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.flush(ctx)
			return
		case <-ticker.C:
			s.flush(ctx)
		}
	}
}

func (s *obsShip) flush(ctx context.Context) {
	s.mu.Lock()
	if s.conn == nil || len(s.ring) == 0 {
		s.mu.Unlock()
		return
	}
	batch := s.ring
	s.ring = nil
	conn := s.conn
	s.mu.Unlock()

	req := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			strAttr("service.name", "graphene-agent"),
			strAttr("graphene.agent", s.agentId),
			strAttr("graphene.role", "agent"),
		}},
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: batch}},
	}}}
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := collogspb.NewLogsServiceClient(conn).Export(sendCtx, req); err != nil {
		// Put the batch back (oldest-first) so a transient failure does
		// not lose the disconnect diagnostics; the ring cap still bounds it.
		s.mu.Lock()
		s.ring = append(batch, s.ring...)
		if over := len(s.ring) - obLogRingMax; over > 0 {
			s.ring = s.ring[over:]
		}
		s.mu.Unlock()
	}
}

// obsHandler tees slog records to the inner handler and the ship.
type obsHandler struct {
	inner slog.Handler
	ship  *obsShip
	attrs []slog.Attr
}

func NewObsHandler(inner slog.Handler, ship *obsShip) *obsHandler {
	return &obsHandler{inner: inner, ship: ship}
}

func (h *obsHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *obsHandler) Handle(ctx context.Context, r slog.Record) error {
	h.ship.add(h.toRecord(r))
	return h.inner.Handle(ctx, r)
}

func (h *obsHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return &obsHandler{inner: h.inner.WithAttrs(as), ship: h.ship, attrs: append(slices.Clip(h.attrs), as...)}
}

func (h *obsHandler) WithGroup(name string) slog.Handler {
	return &obsHandler{inner: h.inner.WithGroup(name), ship: h.ship, attrs: h.attrs}
}

func (h *obsHandler) toRecord(r slog.Record) *logspb.LogRecord {
	ts := r.Time
	if ts.IsZero() {
		ts = time.Now()
	}
	attrs := []*commonpb.KeyValue{strAttr("stream", "agent")}
	for _, a := range h.attrs {
		attrs = append(attrs, strAttr(a.Key, a.Value.String()))
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, strAttr(a.Key, a.Value.String()))
		return true
	})
	num, text := severity(r.Level)
	return &logspb.LogRecord{
		TimeUnixNano:   uint64(ts.UnixNano()), //nolint:gosec // wall clock is positive
		SeverityNumber: num,
		SeverityText:   text,
		Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: r.Message}},
		Attributes:     attrs,
	}
}

// severity maps a slog level to the OTLP severity number and text.
func severity(l slog.Level) (logspb.SeverityNumber, string) {
	switch {
	case l >= slog.LevelError:
		return logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, "ERROR"
	case l >= slog.LevelWarn:
		return logspb.SeverityNumber_SEVERITY_NUMBER_WARN, "WARN"
	case l >= slog.LevelInfo:
		return logspb.SeverityNumber_SEVERITY_NUMBER_INFO, "INFO"
	default:
		return logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG, "DEBUG"
	}
}
