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

	"github.com/graphene-ci/pipeline/id"
	"github.com/graphene-ci/pipeline/wire"
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
	MachineId id.MachineId `json:"machineId"`
	RunId     id.RunId     `json:"runId"`
	Image     ImageRef     `json:"image"`
	// Env is the environment handed to the container (server address,
	// queue name, run-scoped token). Secret VALUES never appear here —
	// only names resolved by the code inside.
	Env map[string]string `json:"env,omitempty"`
}

// Queue returns the task queue the hosted worker must serve.
func (c RunContainer) Queue() string {
	return wire.MachineRunQueue(c.MachineId, c.RunId)
}

// Validate checks the container record structurally.
func (c RunContainer) Validate() error {
	if err := c.MachineId.Validate(); err != nil {
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

// Runtime is the minimal container runtime the agent drives — pull, run,
// stop, status. No docker installation is required; the implementation
// runs OCI images directly. All methods are idempotent.
type Runtime interface {
	// Pull fetches the image (through the server's registry proxy) if it
	// is not already present.
	Pull(ctx context.Context, image ImageRef) error
	// Start launches the container for the record; starting an already
	// running container is a no-op.
	Start(ctx context.Context, c RunContainer) error
	// Stop terminates and removes the container; not-found is not an
	// error.
	Stop(ctx context.Context, c RunContainer) error
	// Status reports the container's lifecycle state.
	Status(ctx context.Context, c RunContainer) (ContainerStatus, error)
}
