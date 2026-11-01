include tools.mk

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

PINS := GO_VERSION GOLANGCI_LINT_VERSION KIND_VERSION KUBECTL_VERSION HELM_VERSION OAPI_CODEGEN_VERSION \
	OPENAPI_SPEC_VALIDATOR_VERSION

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
.PHONY: verify-A1.3 verify-A1.4 verify-A2.1 verify-A2.2
.PHONY: app-images app-deploy e2e-smoke verify-A1.5-static
.PHONY: chaos-install chaos-check chaos-uninstall chaos-suite verify-S4.3

# Chaos experiments (S2.3, S4.3): make chaos-<experiment> SEED=N runs chaos/scripts/<experiment>.sh
# against the running cluster and writes its report to chaos-<experiment>-<seed>.json.
CHAOS_EXPERIMENTS := kill-leader kill-follower restart-shard isolate-replica isolate-leader lose-quorum \
	delay-follower delay-isolate-leader stop-worker restart-coordinator
SEED ?= 1
.PHONY: $(addprefix chaos-,$(CHAOS_EXPERIMENTS))

# Chaos Mesh (S4.3): pins from tools.mk, passed to chaos/mesh/install.sh.
CHAOS_PINS = CHAOS_CONTEXT=kind-$(CLUSTER) CHAOS_NAMESPACE=$(STORAGE_NS) CHAOS_MESH_VERSION='$(CHAOS_MESH_VERSION)' \
	CHAOS_MESH_CHART_SHA256='$(CHAOS_MESH_CHART_SHA256)' CHAOS_MESH_IMAGE_DIGEST='$(CHAOS_MESH_IMAGE_DIGEST)' \
	CHAOS_DAEMON_IMAGE_DIGEST='$(CHAOS_DAEMON_IMAGE_DIGEST)'

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
	@# CI also proves the real pinned-broker TTL/DLX/consumer path (A1.3).
	@if [ "$${GITHUB_ACTIONS:-}" = true ]; then $(MAKE) verify-A1.3; fi
	@if [ "$${GITHUB_ACTIONS:-}" = true ]; then $(MAKE) verify-A1.4; fi
	@if [ "$${GITHUB_ACTIONS:-}" = true ]; then $(MAKE) verify-A2.1; fi
	@if [ "$${GITHUB_ACTIONS:-}" = true ]; then $(MAKE) verify-A2.2; fi
	@if [ "$${GITHUB_ACTIONS:-}" = true ]; then $(MAKE) verify-A2.4; fi

verify-A2.2: ## Verify state-aware retries and confirmed delayed transport handoffs
	bash scripts/verify-A2.2.sh

verify-A2.1: ## Verify cancellation/completion and frozen terminal event schemas
	bash scripts/verify-A2.1.sh

verify-A1.3: ## Verify live messaging against the pinned disposable broker
	bash scripts/verify-A1.3.sh

verify-A1.4: ## Verify live outbox recovery against its own pinned broker
	bash scripts/verify-A1.4.sh

app-images: ## Build/inspect non-root API, matcher and UI images (does not deploy)
	bash scripts/app-images.sh

verify-A1.5-static: ## Verify application and broker-only Helm manifests
	go test -race -count=1 -tags integration -run '^TestApp' -v ./scripts/...

app-deploy: cluster-up ## Build/load images and install the full app on local kind
	bash scripts/app-deploy.sh

e2e-smoke: ## Create/register/match through Traefik and verify reciprocal ownership
	python3 scripts/e2e-smoke.py

# The smoke job already installs Helm/Go; preserve its image-build separation.
ci-smoke: verify-A1.5-static

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

chaos-install: ## Install Chaos Mesh (pinned chart + image digests, namespace-scoped to STORAGE_NS) into kind
	$(CHAOS_PINS) chaos/mesh/install.sh install

chaos-check: ## Check the Chaos Mesh install: pods Ready, no RBAC errors, a smoke NetworkChaos injects and recovers
	$(CHAOS_PINS) chaos/mesh/install.sh check

chaos-uninstall: ## Remove Chaos Mesh: leftover chaos objects, the release, its RBAC and CRDs
	$(CHAOS_PINS) chaos/mesh/install.sh uninstall

$(addprefix chaos-,$(CHAOS_EXPERIMENTS)): ## Run one chaos experiment (SEED=N): fault, heal, settle, judge, JSON report
	CHAOS_CONTEXT=kind-$(CLUSTER) CHAOS_NAMESPACE=$(STORAGE_NS) TOOLS_IMAGE=$(TOOLS_IMAGE) chaos/scripts/$(@:chaos-%=%).sh --seed $(SEED)

chaos-suite: ## S4.3: install Chaos Mesh, run every experiment and both negative controls, evidence in docs/evidence/S4.3
	$(CHAOS_PINS) TOOLS_IMAGE=$(TOOLS_IMAGE) chaos/scripts/suite.sh

verify-S4.3: ## S4.3 Verify: the chaos suite's static checks, then make chaos-suite, all green
	$(CHAOS_PINS) TOOLS_IMAGE=$(TOOLS_IMAGE) scripts/verify-S4.3.sh

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

ci-smoke: ## CI's smoke test on a fresh runner: kind, the storage chart from prebuilt image archives, three leaders, demo 4
	bash scripts/ci-smoke.sh
	bash scripts/app-ci-smoke.sh

gate: ## All nine demos from a clean deployment, twice, with a summary table (asks first)
	bash scripts/demos/gate.sh

.PHONY: verify-A2.4 verify-A2.4-cluster
verify-A2.4: ## Verify paged recovery, wakeup acknowledgement and live fixed-TTL retries
	bash scripts/verify-A2.4.sh

verify-A2.4-cluster: ## Kill test matcher workers at committed boundaries and audit recovery
	bash scripts/verify-A2.4-cluster.sh

.PHONY: verify-A3.1-static verify-A3.1
verify-A3.1-static: ## App demo client, failure propagation and rendered toleration regressions
	bash scripts/verify-A3.1.sh --static

verify-A3.1: ## Clean deployment and app demos 2/3/5/8/9, twice (scoped kind reset)
	bash scripts/verify-A3.1.sh --live
