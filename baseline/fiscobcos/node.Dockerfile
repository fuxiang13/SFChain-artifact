# FISCO BCOS Air-edition node image (built from the locally downloaded v3.17.1 binary,
# does not depend on the fiscoorg/fiscobcos image on Docker Hub; China-network friendly)
# Build context is the fiscobcos/ directory:
#   docker build -f node.Dockerfile -t fisco-bcos-node:v3.17.1 .
# Overseas networks can override the base image:
#   docker build --build-arg BASE_IMAGE=debian:bookworm-slim -f node.Dockerfile -t fisco-bcos-node:v3.17.1 .
ARG BASE_IMAGE=debian:bookworm-slim
FROM ${BASE_IMAGE}

# fisco-bcos runtime dependencies (gcc runtime / OpenMP / compression libs)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libstdc++6 libgcc-s1 libgomp1 zlib1g libsz2 \
    && rm -rf /var/lib/apt/lists/*

COPY bin/fisco-bcos /usr/local/bin/fisco-bcos
RUN chmod +x /usr/local/bin/fisco-bcos

EXPOSE 20200 30300
WORKDIR /data
ENTRYPOINT ["fisco-bcos"]
