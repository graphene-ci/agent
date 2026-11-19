.DEFAULT_GOAL := help

BIN := $(CURDIR)/bin

export PATH := $(BIN):$(PATH)

GOLANGCI_LINT_VERSION := v2.12.2
EASYP_VERSION := v0.16.6
PROTOC_GEN_GO_VERSION := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2

GO_TOOLS := $(BIN)/golangci-lint $(BIN)/easyp $(BIN)/protoc-gen-go $(BIN)/protoc-gen-go-grpc

.PHONY: configure
configure: $(GO_TOOLS) ## Install pinned repository tools into bin

$(BIN)/golangci-lint:
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

$(BIN)/easyp:
	GOBIN=$(BIN) go install github.com/easyp-tech/easyp/cmd/easyp@$(EASYP_VERSION)

$(BIN)/protoc-gen-go:
	GOBIN=$(BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)

$(BIN)/protoc-gen-go-grpc:
	GOBIN=$(BIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

.PHONY: generate
generate: $(GO_TOOLS) ## Generate the agent<->server contract from proto
	$(BIN)/easyp generate

.PHONY: test
test: ## Run tests
	go test ./...

.PHONY: lint
lint: ## Run Go linters
	$(BIN)/golangci-lint run ./...

.PHONY: build
build: ## Build the agent binary
	go build ./cmd/graphene-agent

.PHONY: ver
ver: ## Cut a release: make ver v=X.Y.Z (creates + pushes the tag → release CI)
	@set -eu; \
	v="$(v)"; \
	if ! echo "$$v" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$$'; then echo "usage: make ver v=X.Y.Z" >&2; exit 1; fi; \
	if [ -n "$$(git status --porcelain)" ]; then echo "working tree is dirty — commit first" >&2; exit 1; fi; \
	git tag "v$$v" && git push origin "v$$v"; \
	echo "tagged v$$v — release workflow running"

.PHONY: bump
bump: ## Bump release tag: make bump TYPE=patch|minor|major (default patch)
	@set -eu; \
	type="$(or $(TYPE),patch)"; \
	case "$$type" in patch|minor|major) ;; *) echo "TYPE must be patch|minor|major" >&2; exit 1 ;; esac; \
	cur="$$(git describe --tags --abbrev=0 2>/dev/null | sed -E 's/^v//' || true)"; \
	echo "$$cur" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$$' || cur="0.0.0"; \
	major="$${cur%%.*}"; rest="$${cur#*.}"; minor="$${rest%%.*}"; patch="$${rest##*.}"; \
	case "$$type" in \
	  patch) new="$$major.$$minor.$$((patch+1))" ;; \
	  minor) new="$$major.$$((minor+1)).0" ;; \
	  major) new="$$((major+1)).0.0" ;; \
	esac; \
	echo "bump $$cur -> $$new"; \
	$(MAKE) ver v="$$new"

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-18s %s\n", $$1, $$2}'
