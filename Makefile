.DEFAULT_GOAL := help

BIN := $(CURDIR)/bin

export PATH := $(BIN):$(PATH)

GOLANGCI_LINT_VERSION := v2.12.2

.PHONY: configure
configure: $(BIN)/golangci-lint ## Install pinned repository tools into bin

$(BIN)/golangci-lint:
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

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
	git tag "$$v" && git push origin "$$v"; \
	echo "tagged $$v — release workflow running"

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
