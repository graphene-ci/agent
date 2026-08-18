package runcrt

import (
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// containerSpec builds the OCI runtime spec for a hosted worker
// container: image entrypoint plus the wiring env, own
// pid/mount/ipc/uts/cgroup namespaces, HOST network — the worker must
// reach the server, and user code exists to change the machine.
func containerSpec(cfg imageConfig, env map[string]string) *specs.Spec {
	args := append(append([]string{}, cfg.Entrypoint...), cfg.Cmd...)
	cwd := cfg.WorkingDir
	if cwd == "" {
		cwd = "/"
	}
	return &specs.Spec{
		Version: specs.Version,
		Process: &specs.Process{
			Terminal: false,
			User:     specs.User{UID: 0, GID: 0},
			Args:     args,
			Env:      envList(cfg.Env, env),
			Cwd:      cwd,
		},
		Root: &specs.Root{Path: "merged"},
		Mounts: []specs.Mount{
			{Destination: "/proc", Type: "proc", Source: "proc"},
			{Destination: "/dev", Type: "tmpfs", Source: "tmpfs",
				Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
			{Destination: "/dev/pts", Type: "devpts", Source: "devpts",
				Options: []string{"nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620"}},
			{Destination: "/dev/shm", Type: "tmpfs", Source: "shm",
				Options: []string{"nosuid", "noexec", "nodev", "mode=1777", "size=65536k"}},
			{Destination: "/dev/mqueue", Type: "mqueue", Source: "mqueue",
				Options: []string{"nosuid", "noexec", "nodev"}},
			{Destination: "/sys", Type: "sysfs", Source: "sysfs",
				Options: []string{"nosuid", "noexec", "nodev", "ro"}},
			{Destination: "/sys/fs/cgroup", Type: "cgroup", Source: "cgroup",
				Options: []string{"nosuid", "noexec", "nodev", "relatime", "ro"}},
			// Host networking needs the host's resolver and hosts file.
			{Destination: "/etc/resolv.conf", Type: "bind", Source: "/etc/resolv.conf",
				Options: []string{"bind", "ro"}},
			{Destination: "/etc/hosts", Type: "bind", Source: "/etc/hosts",
				Options: []string{"bind", "ro"}},
		},
		Linux: &specs.Linux{
			Namespaces: []specs.LinuxNamespace{
				{Type: specs.PIDNamespace},
				{Type: specs.IPCNamespace},
				{Type: specs.UTSNamespace},
				{Type: specs.MountNamespace},
				{Type: specs.CgroupNamespace},
				// No NetworkNamespace: host network by design.
			},
			MaskedPaths: []string{
				"/proc/kcore", "/proc/latency_stats", "/proc/timer_list",
				"/proc/timer_stats", "/proc/sched_debug", "/sys/firmware",
			},
			ReadonlyPaths: []string{
				"/proc/asound", "/proc/bus", "/proc/fs", "/proc/irq",
				"/proc/sys", "/proc/sysrq-trigger",
			},
		},
	}
}
