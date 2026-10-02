# fisco-loadgen image: Go + go-sdk v3 (cgo + libbcos-c-sdk)
# Build context is the fiscobcos/ directory:
#   docker build -f loadgen/Dockerfile -t fisco-loadgen .
# Overseas networks can override the base images:
#   docker build --build-arg GO_IMAGE=golang:1.22-bookworm --build-arg RUN_IMAGE=debian:bookworm-slim -f loadgen/Dockerfile -t fisco-loadgen .
ARG GO_IMAGE=golang:1.22-bookworm
ARG RUN_IMAGE=debian:bookworm-slim
FROM ${GO_IMAGE} AS builder

# c-sdk shared library (linked at build time + ldconfig)
COPY loadgen/lib/libbcos-c-sdk.so /usr/local/lib/libbcos-c-sdk.so
RUN ldconfig

ENV CGO_ENABLED=1 \
    GOPROXY=https://proxy.golang.org,direct \
    GOFLAGS=-mod=mod

WORKDIR /src
COPY loadgen/go.mod loadgen/go.sum* ./
RUN go mod download || true
COPY loadgen/ ./
RUN go build -ldflags="-r /usr/local/lib" -o /out/fisco-loadgen .

FROM ${RUN_IMAGE}
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/lib/libbcos-c-sdk.so /usr/local/lib/libbcos-c-sdk.so
COPY --from=builder /out/fisco-loadgen /usr/local/bin/fisco-loadgen
RUN ldconfig
ENTRYPOINT ["fisco-loadgen"]
