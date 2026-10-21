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
