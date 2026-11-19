package session

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	agentpb "github.com/graphene-ci/agent/pkg/proto/agent/v1"
)

// Detacher runs a script OUTSIDE the agent process: the uninstall stops
// the agent's own service, and a child of the agent would die with it.
type Detacher interface {
	Detach(ctx context.Context, script string) error
}

// decommissionUnit is the transient unit the uninstall runs in.
const decommissionUnit = "graphene-agent-decommission"

// decommissionDelay leaves the agent time to answer before the script
// stops it.
const decommissionDelay = 3 * time.Second

// systemdDetacher hands the script to systemd as a transient unit that
// starts after decommissionDelay.
type systemdDetacher struct{}

// NewSystemdDetacher is the production Detacher.
func NewSystemdDetacher() Detacher { return systemdDetacher{} }

func (systemdDetacher) Detach(ctx context.Context, script string) error {
	cmd := exec.CommandContext(ctx, "systemd-run",
		"--unit="+decommissionUnit, "--collect",
		fmt.Sprintf("--on-active=%ds", int(decommissionDelay/time.Second)),
		// A timer fires within a minute by default; leaving promptly matters.
		"--timer-property=AccuracySec=1s",
		"/bin/sh", "-c", script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("systemd-run: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// decommission schedules the server's uninstall script and answers. The
// agent keeps serving until the script stops it; a second Decommission
// while one is scheduled reports the same unit's error from systemd,
// which the server treats as already scheduled.
func (s *Session) decommission(ctx context.Context, cmd *agentpb.Decommission) []*agentpb.SessionRequest {
	if strings.TrimSpace(cmd.GetScript()) == "" {
		return []*agentpb.SessionRequest{result(cmd.GetCommandId(), errors.New("decommission without a script"))}
	}
	s.log.Warn("decommission scheduled: the agent is leaving this machine")
	return []*agentpb.SessionRequest{result(cmd.GetCommandId(), s.detach.Detach(ctx, cmd.GetScript()))}
}
