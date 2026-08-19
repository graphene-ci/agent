// The tailer streams container stdout into the telemetry plane: every
// line of the capture file becomes an OTLP log record at the server's
// door — the raw "inside" of a hosted container, correlated by
// (agent, run). Best effort by design: telemetry never disturbs the
// container, lost lines lose diagnostics, not work.
package session

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"

	"github.com/graphene-ci/agent/pkg/host"
)

const (
	tailPollEvery  = 500 * time.Millisecond
	tailFlushEvery = time.Second
	tailBatchCap   = 256
)

// tailers follows the capture files of the agent's containers for the
// life of the agent, over whatever server connection is currently up.
type tailers struct {
	mu     sync.Mutex
	conn   *grpc.ClientConn
	active map[string]context.CancelFunc
	log    *slog.Logger
}

func newTailers(log *slog.Logger) *tailers {
	return &tailers{active: map[string]context.CancelFunc{}, log: log}
}

// setConn points the tailers at the current session connection; nil
// while reconnecting — batches are dropped, tailing continues.
func (t *tailers) setConn(conn *grpc.ClientConn) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.conn = conn
}

func (t *tailers) client() collogspb.LogsServiceClient {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.conn == nil {
		return nil
	}
	return collogspb.NewLogsServiceClient(t.conn)
}

// start follows one container's capture file until stop or ctx end.
// Idempotent per container.
func (t *tailers) start(ctx context.Context, c host.RunContainer, path string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := string(c.AgentId) + "/" + string(c.RunId)
	if _, running := t.active[key]; running {
		return
	}
	tctx, cancel := context.WithCancel(ctx)
	t.active[key] = cancel
	go t.follow(tctx, c, path)
}

// stop ends one container's tail.
func (t *tailers) stop(c host.RunContainer) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := string(c.AgentId) + "/" + string(c.RunId)
	if cancel, ok := t.active[key]; ok {
		cancel()
		delete(t.active, key)
	}
}

func (t *tailers) follow(ctx context.Context, c host.RunContainer, path string) {
	var (
		offset  int64
		pending []string
		carry   bytes.Buffer
		lastOut = time.Now()
	)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		if client := t.client(); client != nil {
			if err := t.export(ctx, client, c, pending); err != nil {
				t.log.Debug("container log export failed", "error", err)
			}
		}
		pending = pending[:0]
	}
	ticker := time.NewTicker(tailPollEvery)
	defer ticker.Stop()
	defer flush()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		f, err := os.Open(path) //nolint:gosec // the runtime's own capture file
		if err != nil {
			continue
		}
		if _, err := f.Seek(offset, io.SeekStart); err == nil {
			raw, _ := io.ReadAll(io.LimitReader(f, 1<<20))
			offset += int64(len(raw))
			carry.Write(raw)
			for {
				line, err := carry.ReadString('\n')
				if err != nil {
					carry.WriteString(line)
					break
				}
				if line = trimEOL(line); line != "" {
					pending = append(pending, line)
				}
			}
		}
		_ = f.Close()
		if len(pending) >= tailBatchCap || (len(pending) > 0 && time.Since(lastOut) >= tailFlushEvery) {
			flush()
			lastOut = time.Now()
		}
	}
}

func trimEOL(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

func (t *tailers) export(ctx context.Context, client collogspb.LogsServiceClient, c host.RunContainer, lines []string) error {
	now := uint64(time.Now().UnixNano()) //nolint:gosec // wall clock is non-negative
	records := make([]*logspb.LogRecord, 0, len(lines))
	for _, line := range lines {
		records = append(records, &logspb.LogRecord{
			TimeUnixNano: now,
			Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: line}},
			Attributes: []*commonpb.KeyValue{
				strAttr("stream", "container"),
				strAttr("graphene.run", string(c.RunId)),
			},
		})
	}
	req := &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			strAttr("service.name", "graphene-agent"),
			strAttr("graphene.agent", string(c.AgentId)),
			strAttr("graphene.role", "agent"),
		}},
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: records}},
	}}}
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := client.Export(sendCtx, req)
	return err
}

func strAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}}
}
