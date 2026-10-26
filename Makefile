include tools.mk

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

PINS := GO_VERSION GOLANGCI_LINT_VERSION KIND_VERSION KUBECTL_VERSION HELM_VERSION OAPI_CODEGEN_VERSION

# Local cluster. PROFILE=full is the five-node demo cluster; PROFILE=lean is the
# development-only profile for 16 GB machines and only lowers doctor's RAM bar.
CLUSTER     := reliefgrid
KIND_CONFIG ?= deploy/kind/cluster.yaml
PROFILE     ?= full
MEMINFO     ?= /proc/meminfo
ifeq ($(PROFILE),lean)
MIN_AVAILABLE_RAM_GIB ?= 4
else ifeq ($(PROFILE),full)
MIN_AVAILABLE_RAM_GIB ?= 8
else
$(error PROFILE must be full or lean, got '$(PROFILE)')
endif

# Storage on kind (S1.5): the standalone chart deploy/helm/storage.
STORAGE_IMAGE   ?= reliefgrid/storagenode:dev
STORAGE_NS      ?= reliefgrid
STORAGE_RELEASE ?= storage

# Branch whitespace errors are measured from where the branch left this ref.
DIFF_BASE ?= origin/main

.PHONY: help check whitespace-check fmt-check vet lint test gen gen-public gen-internal dev doctor cluster-up cluster-down storage-image storage-reset

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed 's/^[^:]*://' | awk -F':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

check: whitespace-check fmt-check vet lint test ## git diff --check, gofmt, go vet, golangci-lint, go test -race (run before every push)

whitespace-check: ## git diff --check since the merge base with DIFF_BASE (default origin/main)
	@base="$$(git merge-base $(DIFF_BASE) HEAD)" || { echo "whitespace-check: no merge base with $(DIFF_BASE); run git fetch" >&2; exit 1; }; \
		git diff --check "$$base"

fmt-check:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

lint:
	golangci-lint run ./...

test: ## go test -race ./...
	go test -race ./...

gen: gen-public gen-internal ## Regenerate code from contracts/ (must leave no diff)

gen-public: ## contracts/public.yaml -> internal/api/gen (lands with A0.1)
	@echo "gen-public: no contracts/public.yaml yet"

gen-internal: ## contracts/internal.yaml -> internal/internalapi
	go tool oapi-codegen -generate types -package internalapi \
		-o internal/internalapi/types.gen.go contracts/internal.yaml

dev: cluster-up ## Deploy the development stack (for now: just the kind cluster)

cluster-up: ## Create the local kind cluster (no-op if it exists)
	@if kind get clusters 2>/dev/null | grep -qx '$(CLUSTER)'; then \
		echo "cluster-up: $(CLUSTER) already exists"; \
	else \
		kind create cluster --image $(KIND_NODE_IMAGE) --config $(KIND_CONFIG) --wait 3m; \
	fi
	kubectl config use-context kind-$(CLUSTER)

cluster-down: ## Delete the local kind cluster (no-op if absent)
	kind delete cluster --name $(CLUSTER)

doctor: ## Check tool versions, Docker, available RAM (PROFILE=full|lean) and host ports
	@$(foreach p,$(PINS),$(p)=$($(p))) KIND_CONFIG='$(KIND_CONFIG)' MEMINFO='$(MEMINFO)' \
		PROFILE='$(PROFILE)' MIN_AVAILABLE_RAM_GIB='$(MIN_AVAILABLE_RAM_GIB)' scripts/doctor.sh

print-%: ## Print a pin, e.g. make -s print-GOLANGCI_LINT_VERSION
	@echo '$($*)'

storage-image: ## Build the storage node image (storagenode + storagectl) and load it into kind
	docker build -f cmd/storagenode/Dockerfile -t $(STORAGE_IMAGE) .
	kind load docker-image --name $(CLUSTER) $(STORAGE_IMAGE)

# The bootstrap manifests are kept on helm uninstall so wiped disks never
# re-bootstrap by accident (v3.1 §10); deleting them is this deliberate step.
storage-reset: ## Uninstall storage and delete its PVCs and bootstrap manifests (type the namespace to confirm)
	@echo "storage-reset: this deletes every storage PVC and bootstrap manifest in namespace '$(STORAGE_NS)' (context kind-$(CLUSTER))."
	@read -r -p "Type the namespace to confirm: " ans || true; \
		if [ "$${ans:-}" != '$(STORAGE_NS)' ]; then echo "storage-reset: not confirmed; nothing deleted" >&2; exit 1; fi
	helm --kube-context kind-$(CLUSTER) uninstall $(STORAGE_RELEASE) -n $(STORAGE_NS) --ignore-not-found --wait
	kubectl --context kind-$(CLUSTER) -n $(STORAGE_NS) delete pvc -l app.kubernetes.io/component=storagenode --ignore-not-found --wait
	kubectl --context kind-$(CLUSTER) -n $(STORAGE_NS) delete configmap -l app.kubernetes.io/component=storage-manifest --ignore-not-found
