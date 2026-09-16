# Build stage: compile the tptforge CLI against the workspace.
FROM rust:1-slim AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p tpt-stream-cli

# Runtime stage: just the binary plus CA certificates for https sources.
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/tptforge /usr/local/bin/tptforge
WORKDIR /data
ENTRYPOINT ["tptforge"]
