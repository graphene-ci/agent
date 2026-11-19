package session

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	agentpb "github.com/graphene-ci/agent/pkg/proto/agent/v1"
)

type recordingDetacher struct {
	scripts []string
	err     error
}

func (d *recordingDetacher) Detach(_ context.Context, script string) error {
	d.scripts = append(d.scripts, script)
	return d.err
}

// The uninstall runs detached and the server hears back with the outcome
// of the scheduling.
func TestDecommission(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		script    string
		detachErr error
		wantErr   string
		scheduled bool
	}{
		{name: "scheduled", script: "rm -f /usr/local/bin/graphene-agent", scheduled: true},
		{name: "systemd refuses", script: "true", detachErr: errors.New("unit exists"), wantErr: "unit exists", scheduled: true},
		{name: "no script", script: "  ", wantErr: "decommission without a script"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			d := &recordingDetacher{err: c.detachErr}
			s := &Session{log: slog.New(slog.DiscardHandler), detach: d}
			out := s.execute(context.Background(), &agentpb.SessionResponse{Body: &agentpb.SessionResponse_Decommission{
				Decommission: &agentpb.Decommission{CommandId: "cmd-1", Script: c.script},
			}})
			if len(out) != 1 {
				t.Fatalf("answers %v", out)
			}
			res := out[0].GetCommandResult()
			if res.GetCommandId() != "cmd-1" || res.GetError() != c.wantErr {
				t.Fatalf("result %+v, want error %q", res, c.wantErr)
			}
			if (len(d.scripts) == 1) != c.scheduled {
				t.Fatalf("scheduled %v", d.scripts)
			}
		})
	}
}
