.DEFAULT_GOAL := help

BIN := $(CURDIR)/bin
DIST := $(CURDIR)/dist
GOLANGCI_LINT_VERSION := v2.12.2

.PHONY: configure
configure: ## Set up tools in bin/ and download dependencies
	GOBIN=$(BIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go mod download

.PHONY: test
test: ## Run all tests with the race detector
	go test -race ./...

.PHONY: lint
lint: ## Run Go linters
	$(BIN)/golangci-lint run

.PHONY: build
build: ## Build graphene-agent
	mkdir -p $(DIST)
	go build -trimpath -o $(DIST)/graphene-agent ./cmd/graphene-agent

.PHONY: build-testserver
build-testserver: ## Build the local AgentService test server
	mkdir -p $(DIST)
	go build -trimpath -o $(DIST)/graphene-agent-testserver ./cmd/graphene-agent-testserver

.PHONY: help
help: ## List targets with explanations
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-12s\033[0m %s\n", $$1, $$2}'
