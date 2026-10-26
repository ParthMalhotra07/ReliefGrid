# Coordinator image: /coordinator only. Build from the repo root
# (make coordinator-image):
#   docker build -f cmd/coordinator/Dockerfile -t reliefgrid/coordinator:dev .
FROM golang:1.27.2@sha256:820153096fda4de6ac1b7194d12e416b7dd8f4e9a33d17e0345b111b945162d8 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
	CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/coordinator

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/coordinator /
# Numeric, so the kubelet can enforce runAsNonRoot.
USER 65532:65532
ENTRYPOINT ["/coordinator"]
