# Runtime environment for build_chain.sh (generates the 4-node configuration and certificates)
# Why it exists: build_chain.sh is a Linux bash script that depends on openssl/tassl
# to generate certificates and cannot run directly on a Windows host; containerized,
# the whole flow no longer depends on WSL.
# Build context is the fiscobcos/ directory:
#   docker build -f gen.Dockerfile -t fisco-chain-gen .
# Default base image; override with any registry if needed:
#   docker build --build-arg BASE_IMAGE=ubuntu:22.04 -f gen.Dockerfile -t fisco-chain-gen .
ARG BASE_IMAGE=ubuntu:22.04
FROM ${BASE_IMAGE}

# Use the Aliyun apt mirror for speed (China networks); overseas networks can drop this block
RUN sed -i 's|archive.ubuntu.com|archive.ubuntu.com|g; s|security.ubuntu.com|archive.ubuntu.com|g' /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends curl bash openssl ca-certificates util-linux \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /w
# build_chain.sh and the binary are used via volume mounts (kept updatable), no COPY needed
ENTRYPOINT ["bash"]
