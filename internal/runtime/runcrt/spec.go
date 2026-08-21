package runcrt

import (
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// containerSpec builds the OCI runtime spec for a hosted worker
// container. The container is PACKAGING, not isolation: it exists so
// one worker image runs on any machine without per-system binaries.
// The machine stays fully reachable — host network, the host
// filesystem under /host (rbind), and the machine-root env so
// libraries address the machine canonically (pipeline pkg machine).
// Namespaces below are mechanics of the rootfs, not a boundary.
// Env names of the machine contract; the run env may override
// DOCKER_HOST, never the machine root.
const (
	envMachineRoot = "GRAPHENE_MACHINE_ROOT" // mirrored by pipeline pkg machine
	envWorkspace   = "GRAPHENE_WORKSPACE"    // mirrored by pipeline pkg machine
	machineRoot    = "/host"
)

func containerSpec(cfg imageConfig, env map[string]string, workspace string) *specs.Spec {
	withMachine := map[string]string{
		envMachineRoot: machineRoot,
		envWorkspace:   workspace,
	}
	if _, ok := env["DOCKER_HOST"]; !ok {
		// /run, not /var/run: the latter is an ABSOLUTE symlink on the
		// machine and would resolve inside the container's own rootfs.
		withMachine["DOCKER_HOST"] = "unix://" + machineRoot + "/run/docker.sock"
	}
	for k, v := range env {
		withMachine[k] = v
	}
	env = withMachine
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
			// The machine, whole, at a fixed point: files are
			// /host/<path>, the docker socket is
			// /host/var/run/docker.sock, scripts chroot into it.
			{Destination: "/host", Type: "bind", Source: "/",
				Options: []string{"rbind", "rw"}},
			// The workspace: SAME absolute path on the machine and in
			// the container — no path translation exists anywhere (the
			// github-runner lesson). Valid for the container's code,
			// chrooted machine scripts, and the docker daemon at once.
			{Destination: workspace, Type: "bind", Source: workspace,
				Options: []string{"rbind", "rw"}},
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
