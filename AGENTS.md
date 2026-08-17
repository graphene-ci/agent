# AGENTS.md — graphene-agent

The machine agent of graphene vision v3 (`../GRAPHENE.MD` at the org
root): a host for user worker containers, not an executor. The previous
executor implementation (instruction protocol, PTY, artifacts,
at-most-once barrier) lives in the `feat/machine-agent` branch — reuse
its facts and connection code, do not resurrect the instruction
semantics.

## Before making changes

1. Read `../GRAPHENE.MD` — the agent section: bootstrap, image pull
   through the server, per-(machine × run) containers, supervise,
   teardown. A change that contradicts the vision updates the vision
   first.
2. `make lint` and `make test` must be green before push.

## Code rules

- Go; code, names, and comments in English. Commits are Conventional
  Commits, no `Co-Authored-By`.
- The agent listens on no ports: outbound connection only, scoped token.
- The agent never speaks Temporal — the hosted user code does.
- Queue names and identifiers come from the pipeline repository (`wire` / `id`);
  no local duplicates of those conventions.
- Secret values never appear in container env, logs, or specs — names
  only.
- All `Runtime` methods are idempotent (safe to retry).
