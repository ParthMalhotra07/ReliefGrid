include tools.mk

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

PINS := GO_VERSION GOLANGCI_LINT_VERSION KIND_VERSION KUBECTL_VERSION HELM_VERSION OAPI_CODEGEN_VERSION

.PHONY: help check fmt-check vet lint test gen dev doctor

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed 's/^[^:]*://' | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

check: fmt-check vet lint test ## gofmt, go vet, golangci-lint, go test -race (run before every push)

fmt-check:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

lint:
	golangci-lint run ./...

test: ## go test -race ./...
	go test -race ./...

gen: ## Regenerate code from contracts/ (no-op until contracts land)
	@echo "gen: no contracts yet, nothing to generate"

dev: ## Deploy the lean development stack
	@echo "not yet: S0.2 (kind cluster)" >&2; exit 1

doctor: ## Check installed tool versions against tools.mk
	@$(foreach p,$(PINS),$(p)=$($(p))) scripts/doctor.sh

print-%: ## Print a pin, e.g. make -s print-GOLANGCI_LINT_VERSION
	@echo '$($*)'
