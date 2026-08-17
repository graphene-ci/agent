# Graphene machine agent

`graphene-agent` управляет одной Linux-машиной от имени Graphene. Он не слушает
сеть: агент аутентифицируется scoped token, устанавливает исходящее gRPC
соединение и держит `AgentService.Connect`.

Агент реализует контракт `graphene.v1.agent`:

- reconnect с `Hello`, heartbeat и active instruction IDs;
- non-interactive commands и PTY с stdin, resize, signals, timeout и cancel;
- bounded stdout/stderr buffering с sequence и loss counters;
- встроенные machine facts;
- atomic resumable artifact download и resumable artifact upload;
- durable at-most-once barrier для каждого `InstructionId`.

Machine facts — типизированный bounded inventory schema `1`. Агент объявляет в
`Hello` поддержку групп operating system, CPU/NUMA, memory, hardware/PCI,
storage, network, security и execution environment. Сбор опирается на системные
API, `ghw`, `go-sysinfo` и `gopsutil`; внешние команды, Docker CLI, runtime
socket и повышение привилегий не используются. Ошибка, timeout, отсутствие
поддержки и усечение возвращаются отдельно для каждой группы.

Серийные номера, UUID, boot ID, MAC- и IP-адреса считаются чувствительными. Они
возвращаются только когда сервер установил `include_sensitive`, а локальная
конфигурация одновременно разрешает `facts.allow_sensitive`. По умолчанию они
не передаются. Load, свободная память, filesystem usage и счётчики сети не
являются facts: изменяющиеся показатели отправляются как OTEL metrics.

## Конфигурация

Приоритет источников: defaults, YAML file, environment, CLI flags.

```yaml
server:
  address: graphene.example:443
  ca_file: /etc/graphene/ca.pem
  server_name: graphene.example
auth:
  token_file: /run/secrets/graphene-agent-token
state:
  path: /var/lib/graphene-agent/state.db
runtime:
  shell: /bin/sh
  working_directory: /
  probe_timeout: 5s
facts:
  allow_sensitive: false
  max_items: 1024
```

Файл выбирается через `--config` или `GRAPHENE_AGENT_CONFIG`. Для каждого поля
есть `GRAPHENE_AGENT_*` и CLI flag; `graphene-agent --help` перечисляет их.

Токен задаётся одним способом:

- `auth.token_file`, `GRAPHENE_AGENT_AUTH_TOKEN_FILE` или `--token-file`;
- напрямую только через `GRAPHENE_AGENT_TOKEN`.

Одновременное использование token file и прямого env token является ошибкой.
Plaintext transport разрешается только явным `--insecure` и предназначен для
локальной разработки.

Запуск после сборки:

```sh
./dist/graphene-agent \
  --server graphene.example:443 \
  --token-file /run/secrets/graphene-agent-token
```

## Устройство

- `cmd/graphene-agent` — CLI, сигналы процесса и structured logging;
- `internal/session` — TLS/Bearer transport, единственный writer `Connect`,
  heartbeat и reconnect;
- `internal/agent` — диспетчер пяти примитивов и active-operation registry;
- `internal/state` — bbolt barrier для installation и instruction IDs;
- `internal/command`, `internal/output` — процессы, PTY и bounded output;
- `internal/artifact`, `internal/facts` — resumable transfers и типизированный
  bounded inventory без внешних процессов.

Состояние агента является частью at-most-once гарантии. Удаление `state.db`
означает новую установку и создаёт новый `InstallationId`; переносить или
удалять его у работающего агента нельзя.

## Разработка

```sh
make configure
make test
make lint
make build
```

Все инструменты устанавливаются закреплёнными версиями в `./bin`.
