# graphene-agent

`graphene-agent` connects one Linux machine to a Graphene installation. It
listens on no inbound port: the process authenticates with its own agent token
and holds an outbound connection to the server.

The agent reports the machine's facts and state, launches a packaged worker
container for each (machine × run) pair, accepts the server's control commands
and ships telemetry. The same channel carries an interactive PTY, token
rotation and verified self-update. Containers run through `runc`; the connected
machine needs no Docker. The agent does not connect to Temporal — the Temporal
worker runs inside the user's container.

Machine executors have root access to the connected machine: containers are
packaging, not a security boundary. The runtime preserves the agent's permitted
capabilities within its bounding set and permits device access within the parent
cgroup's limits. Machine commands use the host's util-linux `nsenter` to make
mounts visible to host services. Interactive PTY credentials remain separately
controlled by `GRAPHENE_AGENT_PTY_USER`.

The agent's purpose and connection model are described in the
[Graphene docs](https://graphene-ci.github.io/docs/concepts/agents).

## Install

Releases: [github.com/graphene-ci/agent/releases](https://github.com/graphene-ci/agent/releases).

Usually the agent is placed on a machine by the server (cloud user-data or the
ssh install) and self-updates from there. For a standalone install:

```bash
go install github.com/graphene-ci/agent/cmd/graphene-agent@latest   # or @v0.1.0
```

or download a binary from the release page (linux amd64/arm64), unpack and run:

```bash
tar xzf graphene-agent_0.1.0_linux_amd64.tar.gz
sudo install graphene-agent /usr/local/bin/
```

## Layout

| Path | Purpose |
|---|---|
| `cmd/graphene-agent` | build the binary and run the process |
| `internal/session` | outbound connection, commands, PTY, telemetry and self-update |
| `internal/runtime` | worker-container management through `runc` |
| `internal/facts` | Linux-machine inventory facts |
| `internal/config` | agent configuration |
| `proto/agent` | the agent↔server connection contract |

## Build and check

```bash
make configure
make lint
make test
make build
```

`make configure` installs the pinned tools into `bin/` and does not touch the
system environment. `make generate` regenerates `pkg/proto` from
`proto/agent/v1/agent.proto` with them; generated files are not edited by hand.

The facts an agent reports on connect include its physical disks (name, size,
rotational, transport, model, serial and `/dev/disk/by-id` names), read from
`/sys/block`; virtual devices (loop, zram, device-mapper) are left out.

## Release

A pushed semver tag (`vX.Y.Z`) publishes the binaries to a GitHub Release:

```bash
make ver v=0.1.0        # or: make bump TYPE=minor
```

Tag the agent BEFORE graphene — the server image embeds the agent of the same
tag (lockstep).

The opt-in runtime integration test runs on a disposable root VM with runc:
`GRAPHENE_TEST_DISPOSABLE_VM=1 go test -tags=integration ./internal/runtime/runcrt -run TestHostMountIntegration`.
It verifies that a mount created by a packaged executor is visible both on the
host and through the executor file API. It also verifies that `exec.Cmd.Dir` survives namespace entry. CI runs this
test as root on an ephemeral runner; SDK >= 0.2.4 is required. `GRAPHENE_TEST_HOST_SCRIPT` optionally supplies an additional host
script; it executes as root and is only intended for disposable test machines.
