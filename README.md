# graphene-agent

The agent is a **host, not an executor**: it manages one Linux machine on
behalf of [graphene](https://github.com/graphene-ci/graphene). It listens
on no ports — it authenticates with a scoped token and holds a single
outbound gRPC connection to the graphene server.

What it does:

- **bootstrap** — connect, report machine facts, heartbeat; a machine
  record becomes ready when its agent has connected;
- **host user code** — pull the run's worker image through the server and
  run one container per (machine × run) in a minimal container runtime
  (no docker installation required); the code inside is an ordinary
  Temporal worker on the machine's run queue;
- **supervise and tear down** — the container is owned by the run: the
  run's end removes it.

The agent itself never speaks Temporal and has no instruction protocol —
executing things on the machine is the hosted user code's job
(`pipeline.OnAgent` / `pipeline.Action` on the graphene side). The
previous instruction-executor implementation lives in the
`feat/machine-agent` branch; its facts and connection machinery will be
reused.

## Layout

| Package | What it is |
|---|---|
| `pkg/host` | Core types: `RunContainer` (machine × run), `Runtime` (pull/start/stop/status), statuses |
| `cmd/graphene-agent` | The binary (connection loop lands next) |

## Build and check

```bash
make configure   # pinned tools into bin/, nothing global
make lint
make test
make build
```
