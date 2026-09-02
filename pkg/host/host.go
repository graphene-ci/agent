// Package host defines the agent's core types. The agent is a HOST, not
// an executor: it bootstraps the machine's presence (outbound connection
// to the server, scoped token, machine facts), pulls the user's worker
// image through the server, and runs one container per (machine × run) in
// a minimal container runtime. User code inside the container is an
// ordinary Temporal worker on the machine's run queue; the agent never
// speaks Temporal itself.
package host

import (
	"context"
	"errors"

	"github.com/graphene-ci/pipeline/pkg/id"
	"github.com/graphene-ci/pipeline/pkg/wire"
)

// ImageRef names a worker image in a registry, pinned by tag or digest.
// The image version is the run's executor version.
type ImageRef string

// Validate reports whether the image ref is non-empty.
func (r ImageRef) Validate() error {
	if r == "" {
		return errors.New("empty image ref")
	}
	return nil
}

// RunContainer is the agent-side record of one hosted container: the user
// worker of one run on this machine. It is owned by the run — the run's
// end tears it down.
type RunContainer struct {
	AgentId id.AgentId `json:"agentId"`
	RunId   id.RunId   `json:"runId"`
	Image   ImageRef   `json:"image"`
	// Env is the environment handed to the container (server address,
	// queue name, run-scoped token). Secret VALUES never appear here —
	// only names resolved by the code inside.
	Env map[string]string `json:"env,omitempty"`
}

// Queue returns the task queue the hosted worker must serve.
func (c RunContainer) Queue() string {
	return wire.AgentRunQueue(c.AgentId, c.RunId)
}

// Validate checks the container record structurally.
func (c RunContainer) Validate() error {
	if err := c.AgentId.Validate(); err != nil {
		return err
	}
	if err := c.RunId.Validate(); err != nil {
		return err
	}
	return c.Image.Validate()
}

// ContainerStatus is the lifecycle of a hosted container as the agent
// reports it.
type ContainerStatus string

// Container lifecycle states.
const (
	StatusPulling ContainerStatus = "pulling"
	StatusRunning ContainerStatus = "running"
	StatusStopped ContainerStatus = "stopped"
	StatusFailed  ContainerStatus = "failed"
)

// OpLog is where the agent streams the RAW output of a machine operation
// — image-pull progress, runc's own stdout/stderr — line by line, exactly
// what a person running the command by hand would see. Attributed to the
// run and agent so it reads under `graphenectl logs run/<id>` (and the
// agent), turning "it's just slow / stuck" into a visible operation. A nil
// OpLog disables it; emission is always best-effort and never blocks.
type OpLog interface {
	Op(agentId id.AgentId, runId id.RunId, stream, line string)
}

// Runtime is the minimal container runtime the agent drives — pull, run,
// stop, status. No docker installation is required; the implementation
// runs OCI images directly. All methods are idempotent.
type Runtime interface {
	// Pull fetches the container's image (through the server's registry
	// proxy) if it is not already present, streaming pull progress to obs.
	Pull(ctx context.Context, c RunContainer) error
	// Start launches the container for the record; starting an already
	// running container is a no-op.
	Start(ctx context.Context, c RunContainer) error
	// LogPath is the container's stdout/stderr capture file on the
	// machine — the agent tails it into the telemetry stream.
	LogPath(c RunContainer) string
	// Stop terminates and removes the container; not-found is not an
	// error.
	Stop(ctx context.Context, c RunContainer) error
	// Status reports the container's lifecycle state.
	Status(ctx context.Context, c RunContainer) (ContainerStatus, error)
}
