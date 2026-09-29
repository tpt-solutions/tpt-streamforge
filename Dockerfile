# Build stage: compile the tptforge CLI against the workspace.
# TODO(F12): pin by digest once verified, e.g. `rust:1-slim@sha256:<digest>`
# (`docker buildx imagetools inspect rust:1-slim`). Not pinned here because the
# digest could not be verified offline; the tag floats.
FROM rust:1-slim AS build
WORKDIR /src
COPY . .
RUN cargo build --locked --release -p tpt-stream-cli

# Runtime stage: just the binary plus CA certificates for https sources.
# TODO(F12): pin `debian:bookworm-slim@sha256:<digest>` (see above).
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 --shell /usr/sbin/nologin tptforge \
    && mkdir /data && chown tptforge:tptforge /data
COPY --from=build /src/target/release/tptforge /usr/local/bin/tptforge
USER tptforge
WORKDIR /data
ENTRYPOINT ["tptforge"]
