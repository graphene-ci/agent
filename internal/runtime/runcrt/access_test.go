package runcrt

import (
	"slices"
	"testing"

	"github.com/moby/sys/capability"
)

func TestMachineAccessIsBoundedByAgent(t *testing.T) {
	spec := containerSpec(imageConfig{}, nil, t.TempDir())
	if err := machineAccess(spec); err != nil {
		t.Fatal(err)
	}
	caps, err := capability.NewPid2(0)
	if err != nil {
		t.Fatal(err)
	}
	if err := caps.Load(); err != nil {
		t.Fatal(err)
	}
	got := spec.Process.Capabilities
	if !slices.Equal(got.Bounding, got.Permitted) || !slices.Equal(got.Permitted, got.Effective) {
		t.Fatal("executor capability sets differ")
	}
	if slices.Contains(got.Effective, "CAP_SYS_ADMIN") !=
		(caps.Get(capability.PERMITTED, capability.CAP_SYS_ADMIN) && caps.Get(capability.BOUNDING, capability.CAP_SYS_ADMIN)) {
		t.Fatal("mount privilege must match the agent's available privilege")
	}
	for _, ns := range spec.Linux.Namespaces {
		if ns.Type == "pid" || ns.Type == "user" {
			t.Fatal("machine commands require the host PID and user namespace")
		}
	}
}
