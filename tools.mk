# Tool version pins: the single source of truth for `make doctor` and CI.
# Change a pin here, in its own PR, and both owners update their machines.
# GO_VERSION must equal the `toolchain` line in go.mod; OAPI_CODEGEN_VERSION
# must equal the oapi-codegen requirement in go.mod (it runs as `go tool`).

GO_VERSION            := 1.27.2
GOLANGCI_LINT_VERSION := v2.15.0
KIND_VERSION          := v0.33.0
KUBECTL_VERSION       := v1.37.1
HELM_VERSION          := v3.22.1
OAPI_CODEGEN_VERSION  := v2.8.0
# Python openapi-spec-validator: verify-A0.1 validates contracts/public.yaml with it.
OPENAPI_SPEC_VALIDATOR_VERSION := 0.9.0
# k6 for the S5.2 load report (loadtest/k6), run on the host and pinned to fixed cores
# with taskset. Not checked by make doctor: only the load runs need it. K6_SHA256 is the
# linux-amd64 tarball's, from the release's k6-<version>-checksums.txt.
K6_VERSION            := v2.3.0
K6_SHA256             := 39c3117b6af817592dcd0ce4242105c0a7af10948c2a425306f0be8f7a8a8ab1

# sha256 of the linux/amd64 downloads CI installs (scripts/ci-tools.sh): the kind
# binary, the kubectl binary and the helm tarball. CI checks each download
# against these, never against a checksum fetched from the same server. Change
# them together with the version above.
KIND_SHA256           := aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d
KUBECTL_SHA256        := 65691ff77eb6fa44c908b77a1082c9f092c3b9733b5cefabec0d1104890e21a8
HELM_SHA256           := 896cf63684a42a723d61c980369d6e588330927deb13676ca0d85006dd6da0ea

# The kindest/node image kind $(KIND_VERSION) was released with, pinned by digest
# as kind's release notes require. Change it together with KIND_VERSION.
KIND_NODE_IMAGE       := kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5

# Chaos Mesh for the S4.3 suite (make chaos-install, chaos/mesh/install.sh): the
# chart by sha256 and both images by their linux/amd64 manifest digests (the
# entries of the v2.8.7 multi-arch indexes that kind nodes run). Change all four
# together.
CHAOS_MESH_VERSION        := 2.8.7
CHAOS_MESH_CHART_SHA256   := c963e0f4ed2e30c1c7e236477f1c31fcf988b171991801bbe392546948436d38
CHAOS_MESH_IMAGE_DIGEST   := sha256:e972b7d995c117f8728ed9b792685b4e45ab9c6eb2e92d766a3040aa5987428d
CHAOS_DAEMON_IMAGE_DIGEST := sha256:3f2108e42a7ced02ef57fd3f0dd8dc71067689f3174ae58a5b0bb8dc31465d55

# Dispatch build/runtime tooling (A4.1); npm dependencies have exact lockfile pins.
NODE_VERSION := 24.16.0
# A4.2: private local monitoring images, pinned to the verified Linux/amd64 manifests.
PROMETHEUS_VERSION := v3.16.0
PROMETHEUS_IMAGE_DIGEST := sha256:89c1a644817e9ad70fa2af76c84898e327c1de7063735f962e8313b1e28672c3
GRAFANA_VERSION := 13.3.0
GRAFANA_IMAGE_DIGEST := sha256:6e5d306338670f381a942f1c50d3c266bf71531e5ddd9ee4fc1a133dac5ce63d
