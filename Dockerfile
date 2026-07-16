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

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build -p oxidesfu-server --bin oxidesfu-server --release --locked

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
COPY --from=builder /src/target/release/oxidesfu-server /app/oxidesfu-server

RUN chmod +x /app/oxidesfu-server && chown -R oxidesfu:oxidesfu /app

USER oxidesfu:oxidesfu

# HTTP/Twirp/WebSocket signaling endpoint. The server's development default
# binds loopback-only, so the image explicitly binds all container interfaces.
EXPOSE 7880/tcp

ENV RUST_LOG=info

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD ["curl", "--fail", "--silent", "http://127.0.0.1:7880/healthz"]

ENTRYPOINT ["/usr/bin/tini", "--", "/app/oxidesfu-server"]
CMD ["--dev", "--bind", "0.0.0.0:7880"]
