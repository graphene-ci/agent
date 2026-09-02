package session

import (
	"context"
	"log/slog"
	"testing"
)

func TestSeverityMapping(t *testing.T) {
	cases := []struct {
		lvl  slog.Level
		text string
	}{
		{slog.LevelDebug, "DEBUG"},
		{slog.LevelInfo, "INFO"},
		{slog.LevelWarn, "WARN"},
		{slog.LevelError, "ERROR"},
	}
	for _, c := range cases {
		if _, got := severity(c.lvl); got != c.text {
			t.Errorf("severity(%v) = %q, want %q", c.lvl, got, c.text)
		}
	}
}

// A record with no live connection must be kept, not dropped: the
// disconnect diagnostics are the whole point.
func TestObsHandlerBuffersWhileDisconnected(t *testing.T) {
	ship := NewObsShip("edge-1")
	h := NewObsHandler(slog.NewTextHandler(discard{}, nil), ship)
	log := slog.New(h)
	log.Error("session ended, reconnecting", "backoff", "1s")
	ship.mu.Lock()
	n := len(ship.ring)
	ship.mu.Unlock()
	if n != 1 {
		t.Fatalf("ring holds %d records, want 1 (no conn = buffer, not drop)", n)
	}
	// flush with no conn is a no-op that keeps the backlog.
	ship.flush(context.Background())
	ship.mu.Lock()
	n = len(ship.ring)
	ship.mu.Unlock()
	if n != 1 {
		t.Fatalf("after flush with no conn ring holds %d, want 1 (kept for reconnect)", n)
	}
}

// The ring is bounded: an overflow drops the OLDEST, a live tail beats a
// stale backlog.
func TestObsRingDropsOldest(t *testing.T) {
	ship := NewObsShip("edge-1")
	h := NewObsHandler(slog.NewTextHandler(discard{}, nil), ship)
	log := slog.New(h)
	for i := 0; i < obLogRingMax+10; i++ {
		log.Info("line")
	}
	ship.mu.Lock()
	n := len(ship.ring)
	ship.mu.Unlock()
	if n != obLogRingMax {
		t.Fatalf("ring holds %d, want capped at %d", n, obLogRingMax)
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
