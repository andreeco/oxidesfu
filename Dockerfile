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
    tini \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --uid 10001 --shell /usr/sbin/nologin oxidesfu

WORKDIR /app
COPY --from=builder /src/target/release/oxidesfu-server /app/oxidesfu-server

RUN chmod +x /app/oxidesfu-server && chown -R oxidesfu:oxidesfu /app

USER oxidesfu:oxidesfu

# HTTP/Twirp/WS signaling endpoint.
# Tracked compose defaults to 7881; private LiveKit-name remote compose may bind 7885
# to match an existing Caddy upstream. EXPOSE is documentation only.
EXPOSE 7881/tcp
EXPOSE 7885/tcp

ENV RUST_LOG=info

ENTRYPOINT ["/usr/bin/tini", "--", "/app/oxidesfu-server"]
CMD []
