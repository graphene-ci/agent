// Package facts collects the bounded machine inventory reported in the
// session Hello. Standard library and /proc only — no external programs.
package facts

import (
	"math"
	"net"
	"os"
	"runtime"

	"github.com/graphene-ci/agent/pkg/agentpb"
)

const maxAddresses = 32

// Collect gathers the machine facts. Collection is best-effort: a field
// that cannot be read is left empty, never an error — facts must not
// keep an agent from connecting.
func Collect() *agentpb.Facts {
	f := &agentpb.Facts{
		Os:   runtime.GOOS,
		Arch: runtime.GOARCH,
		Cpus: uint32(min(runtime.NumCPU(), math.MaxUint16)), //nolint:gosec // clamped to MaxUint16 right here
	}
	if hostname, err := os.Hostname(); err == nil {
		f.Hostname = hostname
	}
	f.MemoryBytes = totalMemoryBytes()
	f.Addresses = collectAddresses()
	return f
}

func collectAddresses() []string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var out []string
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok || ipNet.IP.IsLinkLocalUnicast() {
				continue
			}
			out = append(out, ipNet.IP.String())
			if len(out) == maxAddresses {
				return out
			}
		}
	}
	return out
}
