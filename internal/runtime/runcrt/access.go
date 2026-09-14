package runcrt

import (
	"fmt"
	"strings"

	"github.com/moby/sys/capability"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// machineAccess preserves the agent's available privileges. The executor is
// machine tooling, not a sandbox; parent cgroup restrictions still apply.
func machineAccess(spec *specs.Spec) error {
	caps, err := capability.NewPid2(0)
	if err != nil {
		return fmt.Errorf("read agent capabilities: %w", err)
	}
	if err := caps.Load(); err != nil {
		return fmt.Errorf("load agent capabilities: %w", err)
	}
	var names []string
	for _, cap := range capability.ListKnown() {
		if caps.Get(capability.PERMITTED, cap) && caps.Get(capability.BOUNDING, cap) {
			names = append(names, "CAP_"+strings.ToUpper(cap.String()))
		}
	}
	spec.Process.Capabilities = &specs.LinuxCapabilities{
		Bounding: names, Permitted: names, Effective: names,
	}
	spec.Linux.Resources = &specs.LinuxResources{
		Devices: []specs.LinuxDeviceCgroup{{Allow: true, Access: "rwm"}},
	}
	return nil
}
