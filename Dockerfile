# syntax=docker/dockerfile:1.6

############################
# Build stage
############################
FROM rust:trixie AS builder

ENV CARGO_TARGET_DIR=/src/target

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# The target directory is a BuildKit cache mount, so copy the finished binary
# outside it for the runtime stage after the build completes.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build -p oxidesfu-server --bin oxidesfu-server --release --locked && \
    install -D /src/target/release/oxidesfu-server /out/oxidesfu-server

############################
# Runtime stage
############################
FROM debian:trixie-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tini \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --uid 10001 --shell /usr/sbin/nologin oxidesfu

WORKDIR /app
COPY --from=builder /out/oxidesfu-server /app/oxidesfu-server
COPY docker-entrypoint.sh /usr/local/bin/oxidesfu-entrypoint

RUN chmod +x /app/oxidesfu-server /usr/local/bin/oxidesfu-entrypoint && \
    chown -R oxidesfu:oxidesfu /app

# Start as root only long enough to copy Docker secret mounts into an
# oxidesfu-owned private directory; docker-entrypoint.sh then drops privileges
# before executing the server.

# HTTP/Twirp/WebSocket signaling endpoint. The native development default is
# loopback-only, so the image supplies a container-safe default through the
# normal environment-based configuration path. Deployments can override this
# value (for example, `OXIDESFU_BIND=0.0.0.0:7885`) without a CLI argument
# taking precedence.
EXPOSE 7880/tcp
# Docker-network TLS TURN listener used by the existing Caddy L4 upstream.
EXPOSE 443/tcp

ENV RUST_LOG=info \
    OXIDESFU_BIND=0.0.0.0:7880

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["curl", "--fail", "--silent", "http://127.0.0.1:7880/healthz"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/oxidesfu-entrypoint", "/app/oxidesfu-server"]
