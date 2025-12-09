.PHONY: help build run clean fmt lint test check generate

# Default target
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build the stats card generator binary
	@echo "Building generate-stats-card..."
	@go build -o bin/generate-stats-card generate-stats-card.go
	@echo "Build complete: bin/generate-stats-card"

run: ## Generate stats card (requires GITHUB_TOKEN)
	@echo "Generating stats card..."
	@go run generate-stats-card.go
	@echo "Stats card generated: stats-card.svg"

clean: ## Remove generated files and build artifacts
	@echo "Cleaning up..."
	@rm -f stats-card.svg
	@rm -rf bin/
	@rm -f .*.swp
	@echo "Clean complete"

fmt: ## Format Go code
	@echo "Formatting code..."
	@gofmt -w -s .
	@echo "Format complete"

lint: ## Run linters
	@echo "Running linters..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed. Run: curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin"; \
		exit 1; \
	fi
	@echo "Lint complete"

test: ## Run tests
	@echo "Running tests..."
	@go test -v -race -coverprofile=coverage.out ./...
	@echo "Test complete"

check: fmt lint ## Run formatting and linting checks
	@echo "All checks passed"

generate: clean run ## Clean and generate new stats card
	@echo "Stats card regenerated successfully"

install-tools: ## Install development tools
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "Tools installed"

.DEFAULT_GOAL := help
