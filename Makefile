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

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "%-18s %s\n", $$1, $$2}'
