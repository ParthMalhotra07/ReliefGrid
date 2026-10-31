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
COORDINATOR_IMAGE ?= reliefgrid/coordinator:dev
TOOLS_IMAGE     ?= reliefgrid/tools:dev
STORAGE_NS      ?= reliefgrid
STORAGE_RELEASE ?= storage

# Branch whitespace errors are measured from where the branch left this ref.
DIFF_BASE ?= origin/main

.PHONY: help check whitespace-check fmt-check vet lint test gen gen-check gen-public gen-internal ci-smoke dev doctor cluster-up cluster-down storage-image coordinator-image tools-image storage-replace storage-reset placement-check
.PHONY: demo-1 demo-2 demo-3 demo-4 demo-5 demo-6 demo-7 demo-8 demo-9 demo-reset gate

help: ## List targets
	@grep -E '^[a-z0-9 -]+:.*## ' $(MAKEFILE_LIST) | sed 's/^[^:]*://' | awk -F':.*## ' '{printf "  %-17s %s\n", $$1, $$2}'

check: whitespace-check fmt-check vet lint test ## git diff --check, gofmt, go vet, golangci-lint, go test -race (run before every push)

whitespace-check: ## git diff --check since the merge base with DIFF_BASE (default origin/main)
	@base="$$(git merge-base $(DIFF_BASE) HEAD)" || { echo "whitespace-check: no merge base with $(DIFF_BASE); run git fetch" >&2; exit 1; }; \
		git diff --check "$$base"

fmt-check:
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

# The chaos build tag adds `invcheck inject` (tools image only); vet, lint
# and test that build too.
vet:
	go vet ./...
	go vet -tags chaos ./cmd/invcheck/...

lint:
	golangci-lint run ./...
	golangci-lint run --build-tags chaos ./cmd/invcheck/...

test: ## go test -race ./... (and cmd/invcheck with the chaos tag)
	go test -race ./...
	go test -race -tags chaos ./cmd/invcheck/...

gen: gen-public gen-internal ## Regenerate code from contracts/ (must leave no diff)

# Only the generated paths are compared, so unrelated uncommitted work does not fail it.
gen-check: gen ## Contract drift: regenerate from contracts/ and fail on any difference (CI's check job)
	git diff --exit-code -- internal/api/gen internal/internalapi

gen-public: ## contracts/public.yaml -> internal/api/gen
	mkdir -p internal/api/gen
	go tool oapi-codegen \
		-generate types \
		-package gen \
		-o internal/api/gen/types.gen.go \
		contracts/public.yaml

gen-internal: ## contracts/internal.yaml -> internal/internalapi
	go tool oapi-codegen -generate types -package internalapi \
		-o internal/internalapi/types.gen.go contracts/internal.yaml

verify-A0.1: ## Run the A0.1 Verify script
	bash scripts/verify-A0.1.sh

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

tools-image: ## Build the tools image (invcheck with the chaos tag) and load it into kind
	docker build -f cmd/invcheck/Dockerfile -t $(TOOLS_IMAGE) .
	kind load docker-image --name $(CLUSTER) $(TOOLS_IMAGE)

coordinator-image: ## Build the coordinator image and load it into kind
	docker build -f cmd/coordinator/Dockerfile -t $(COORDINATOR_IMAGE) .
	kind load docker-image --name $(CLUSTER) $(COORDINATOR_IMAGE)

storage-replace: ## Run storagectl replace SHARD=N POD=<pod> as a Job (M7 disk replacement; rerun to resume)
	KUBE_CONTEXT=kind-$(CLUSTER) NAMESPACE=$(STORAGE_NS) SHARD='$(SHARD)' POD='$(POD)' CRASH_AT='$(CRASH_AT)' scripts/storage-replace.sh

placement-check: ## Fail if two replicas of a storage shard share a worker (or one is off the data workers)
	KUBE_CONTEXT=kind-$(CLUSTER) NAMESPACE=$(STORAGE_NS) scripts/placement-check.sh

# The bootstrap manifests belong to storagectl, not Helm, so helm uninstall
# leaves them and wiped disks never re-bootstrap by accident (v3.1 §10);
# deleting them is this deliberate step.
storage-reset: ## Uninstall storage and delete its PVCs and bootstrap manifests (type the namespace to confirm)
	@echo "storage-reset: this deletes every storage PVC and bootstrap manifest in namespace '$(STORAGE_NS)' (context kind-$(CLUSTER))."
	@read -r -p "Type the namespace to confirm: " ans || true; \
		if [ "$${ans:-}" != '$(STORAGE_NS)' ]; then echo "storage-reset: not confirmed; nothing deleted" >&2; exit 1; fi
	helm --kube-context kind-$(CLUSTER) uninstall $(STORAGE_RELEASE) -n $(STORAGE_NS) --ignore-not-found --wait
	kubectl --context kind-$(CLUSTER) -n $(STORAGE_NS) delete pvc -l app.kubernetes.io/name=storage,app.kubernetes.io/component=storagenode --ignore-not-found --wait
	kubectl --context kind-$(CLUSTER) -n $(STORAGE_NS) delete configmap -l app.kubernetes.io/name=storage,app.kubernetes.io/component=storage-manifest --ignore-not-found

# The demo gate (S3.2, v3.1 §28): scripts/demos/. Each demo prints PASS, FAIL, NOT IMPLEMENTED
# or PENDING and writes docs/evidence/<runId>/demo-N.txt (DEMO_RUN_ID; default one per run).
# One run directory per make invocation: make demo-6 demo-7 writes both demos' evidence under the
# same DEMO_RUN_ID (computed once, here). A DEMO_RUN_ID from the caller wins.
ifeq ($(origin DEMO_RUN_ID),undefined)
DEMO_RUN_ID := $(shell date -u +%Y%m%dT%H%M%SZ)-$(shell hostname -s)
endif

demo-1 demo-2 demo-3 demo-4 demo-5 demo-6 demo-7 demo-8 demo-9: ## Run §28 demos (any combination) with assertions and evidence
	DEMO_RUN_ID='$(DEMO_RUN_ID)' bash scripts/demos/$@.sh

demo-reset: ## Clean deployment for the demos: recreate the kind cluster, images, storage (asks first)
	bash scripts/demos/reset.sh

ci-smoke: verify-storage-labels ## CI's smoke test on a fresh runner: kind, the storage chart from prebuilt image archives, three leaders, demo 4
	bash scripts/ci-smoke.sh

gate: ## All nine demos from a clean deployment, twice, with a summary table (asks first)
	bash scripts/demos/gate.sh

.PHONY: verify-storage-labels
verify-storage-labels: ## Strictly render storage YAML and verify RF3 pod/selector labels
	go test -race -count=1 -tags integration -run '^TestStorageChartUniquePodLabelsPreserveSelectors$$' -v ./scripts/...
