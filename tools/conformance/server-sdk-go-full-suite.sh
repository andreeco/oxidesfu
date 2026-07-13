#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SERVER_SDK_GO="${SERVER_SDK_GO:-$ROOT/../othercode/server-sdk-go}"

HOST="${OXIDESFU_HOST:-127.0.0.1}"
PORT="${OXIDESFU_PORT:-7880}"
LOCAL_HTTP_URL="http://$HOST:$PORT"
API_KEY="${LIVEKIT_API_KEY:-devkey}"
API_SECRET="${LIVEKIT_API_SECRET:-secret}"
REUSE_SERVER="${OXIDESFU_REUSE_SERVER:-false}"
ALLOW_FAILURE="${OXIDESFU_DISCOVERY_ALLOW_FAILURE:-false}"
LOG_DIR="${OXIDESFU_DISCOVERY_LOG_DIR:-$ROOT/target/conformance}"
GO_TEST_EXTRA_FLAGS="${OXIDESFU_DISCOVERY_GO_TEST_EXTRA_FLAGS:--p 1}"
GO_TEST_SKIP_PATTERN="${OXIDESFU_DISCOVERY_GO_TEST_SKIP_PATTERN-}"
TURN_MODE="${OXIDESFU_DISCOVERY_TURN_MODE:-auto}"
# `oxide` exercises the in-process TURN runtime that production OxideSFU uses.
# `coturn` remains available only for legacy external-TURN comparison runs.
TURN_BACKEND="${OXIDESFU_DISCOVERY_TURN_BACKEND:-oxide}"
TURN_HOST="${OXIDESFU_DISCOVERY_TURN_HOST:-127.0.0.1}"
TURN_PORT="${OXIDESFU_DISCOVERY_TURN_PORT:-34790}"
TURN_RELAY_MIN_PORT="${OXIDESFU_DISCOVERY_TURN_RELAY_MIN_PORT:-31000}"
TURN_RELAY_MAX_PORT="${OXIDESFU_DISCOVERY_TURN_RELAY_MAX_PORT:-31050}"
TURN_USERNAME="${OXIDESFU_DISCOVERY_TURN_USERNAME:-$API_KEY}"
TURN_PASSWORD="${OXIDESFU_DISCOVERY_TURN_PASSWORD:-$API_SECRET}"
# Optional extra peer-policy entries for local TURN harnesses.
# For the owned runtime we default this to loopback in start_local_turn_if_enabled().
# For coturn, entries must be plain IP/IP-IP (CIDR is unsupported by coturn).
TURN_ALLOWED_PEER_CIDRS="${OXIDESFU_DISCOVERY_TURN_ALLOWED_PEER_CIDRS:-}"
EXTERNAL_TURN_HOST="${OXIDESFU_DISCOVERY_EXTERNAL_TURN_HOST:-}"
EXTERNAL_TURN_UDP_PORT="${OXIDESFU_DISCOVERY_EXTERNAL_TURN_UDP_PORT:-}"
EXTERNAL_TURN_TLS_PORT="${OXIDESFU_DISCOVERY_EXTERNAL_TURN_TLS_PORT:-}"
EXTERNAL_TURN_USERNAME="${OXIDESFU_DISCOVERY_EXTERNAL_TURN_USERNAME:-$TURN_USERNAME}"
EXTERNAL_TURN_PASSWORD="${OXIDESFU_DISCOVERY_EXTERNAL_TURN_PASSWORD:-$TURN_PASSWORD}"
TURN_PID=""
TURN_CONFIG_FILE=""
TURN_LOG_FILE="/tmp/oxidesfu-conformance-turn.log"
SERVER_PID=""

if [[ "$REUSE_SERVER" == "true" ]]; then
  HTTP_URL="${LIVEKIT_URL:-$LOCAL_HTTP_URL}"
else
  HTTP_URL="$LOCAL_HTTP_URL"
  if [[ -n "${LIVEKIT_URL:-}" && "$LIVEKIT_URL" != "$HTTP_URL" ]]; then
    echo "warning: ignoring LIVEKIT_URL=$LIVEKIT_URL because OXIDESFU_REUSE_SERVER=false; using local spawned server at $HTTP_URL" >&2
  fi
fi

cleanup() {
  if [[ -n "$SERVER_PID" ]]; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$TURN_PID" ]]; then
    kill "$TURN_PID" >/dev/null 2>&1 || true
    wait "$TURN_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "$TURN_CONFIG_FILE" ]]; then
    rm -f "$TURN_CONFIG_FILE"
  fi
}
trap cleanup EXIT

if [[ ! -d "$SERVER_SDK_GO" ]]; then
  echo "server-sdk-go checkout not found: $SERVER_SDK_GO" >&2
  echo "Set SERVER_SDK_GO=/path/to/server-sdk-go" >&2
  exit 2
fi

wait_for_http() {
  local deadline
  deadline=$((SECONDS + 45))
  while (( SECONDS < deadline )); do
    local status
    status="$(curl -sS -o /dev/null -w '%{http_code}' "$HTTP_URL/rtc/validate" 2>/dev/null || true)"
    if [[ "$status" != "000" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "OxideSFU did not become reachable at $HTTP_URL within 45s" >&2
  return 1
}

configure_external_turn_if_requested() {
  if [[ -n "${OXIDESFU_ICE_SERVERS_JSON:-}" ]]; then
    return 0
  fi

  if [[ -z "$EXTERNAL_TURN_HOST" ]]; then
    return 0
  fi

  if [[ "$TURN_MODE" == "auto" ]]; then
    TURN_MODE="off"
  fi
  export OXIDESFU_TURN_ENABLED=false

  local urls=()
  if [[ -n "$EXTERNAL_TURN_UDP_PORT" ]]; then
    urls+=("stun:$EXTERNAL_TURN_HOST:$EXTERNAL_TURN_UDP_PORT")
    urls+=("turn:$EXTERNAL_TURN_HOST:$EXTERNAL_TURN_UDP_PORT?transport=udp")
  fi
  if [[ -n "$EXTERNAL_TURN_TLS_PORT" ]]; then
    urls+=("turns:$EXTERNAL_TURN_HOST:$EXTERNAL_TURN_TLS_PORT?transport=tcp")
  fi

  if [[ ${#urls[@]} -eq 0 ]]; then
    echo "OXIDESFU_DISCOVERY_EXTERNAL_TURN_HOST is set but no external TURN ports were provided" >&2
    echo "Set OXIDESFU_DISCOVERY_EXTERNAL_TURN_UDP_PORT and/or OXIDESFU_DISCOVERY_EXTERNAL_TURN_TLS_PORT" >&2
    exit 2
  fi

  local url_json=""
  local first="true"
  for url in "${urls[@]}"; do
    if [[ "$first" == "true" ]]; then
      url_json="\"$url\""
      first="false"
    else
      url_json="$url_json,\"$url\""
    fi
  done

  OXIDESFU_ICE_SERVERS_JSON="[{\"urls\":[${url_json}],\"username\":\"$EXTERNAL_TURN_USERNAME\",\"credential\":\"$EXTERNAL_TURN_PASSWORD\"}]"
  export OXIDESFU_ICE_SERVERS_JSON
  echo "Configured external TURN ICE servers for OxideSFU from OXIDESFU_DISCOVERY_EXTERNAL_TURN_*"
}

ensure_turn_ice_servers_json() {
  if [[ -n "${OXIDESFU_ICE_SERVERS_JSON:-}" ]]; then
    return 0
  fi

  OXIDESFU_ICE_SERVERS_JSON="[{\"urls\":[\"stun:$TURN_HOST:$TURN_PORT\"]},{\"urls\":[\"turn:$TURN_HOST:$TURN_PORT?transport=udp\"],\"username\":\"$TURN_USERNAME\",\"credential\":\"$TURN_PASSWORD\"}]"
  export OXIDESFU_ICE_SERVERS_JSON
}

start_coturn_backend() {
  local turn_bin=""
  if command -v turnserver >/dev/null 2>&1; then
    turn_bin="turnserver"
  elif command -v coturn >/dev/null 2>&1; then
    turn_bin="coturn"
  fi

  if [[ -z "$turn_bin" ]]; then
    return 1
  fi

  TURN_CONFIG_FILE="/tmp/oxidesfu-conformance-turn-$TIMESTAMP.conf"
  {
    cat <<EOF
listening-ip=$TURN_HOST
relay-ip=$TURN_HOST
realm=oxidesfu.local
lt-cred-mech
user=$TURN_USERNAME:$TURN_PASSWORD
allow-loopback-peers
allowed-peer-ip=$TURN_HOST
server-relay
no-cli
no-tls
no-dtls
no-tcp
no-multicast-peers
min-port=$TURN_RELAY_MIN_PORT
max-port=$TURN_RELAY_MAX_PORT
listening-port=$TURN_PORT
EOF

    if [[ -n "$TURN_ALLOWED_PEER_CIDRS" ]]; then
      IFS=',' read -r -a allowed_cidrs <<<"$TURN_ALLOWED_PEER_CIDRS"
      for cidr in "${allowed_cidrs[@]}"; do
        cidr="${cidr//[[:space:]]/}"
        if [[ -z "$cidr" ]]; then
          continue
        fi
        if [[ "$cidr" == */* ]]; then
          echo "warning: skipping CIDR '$cidr' for coturn allowed-peer-ip (coturn expects IP or IP-IP range)" >&2
          continue
        fi
        echo "allowed-peer-ip=$cidr"
      done
    fi
  } >"$TURN_CONFIG_FILE"

  echo "Starting local TURN ($turn_bin) on $TURN_HOST:$TURN_PORT ..."
  "$turn_bin" -c "$TURN_CONFIG_FILE" >"$TURN_LOG_FILE" 2>&1 &
  TURN_PID="$!"
  sleep 1

  if ! kill -0 "$TURN_PID" >/dev/null 2>&1; then
    TURN_PID=""
    return 1
  fi

  return 0
}

start_local_turn_if_enabled() {
  if [[ "$REUSE_SERVER" == "true" || "$TURN_MODE" == "off" ]]; then
    return 0
  fi

  local selected_backend="$TURN_BACKEND"
  if [[ "$selected_backend" == "auto" ]]; then
    selected_backend="oxide"
  fi

  case "$selected_backend" in
    oxide)
      local owned_allow_peer_cidrs="$TURN_ALLOWED_PEER_CIDRS"
      if [[ -z "$owned_allow_peer_cidrs" ]]; then
        # Same-host ForceTLS exercises relay permissions against loopback peers.
        owned_allow_peer_cidrs="127.0.0.0/8"
      fi
      export OXIDESFU_TURN_ENABLED=true
      export OXIDESFU_TURN_DOMAIN="$TURN_HOST"
      export OXIDESFU_TURN_BIND="$TURN_HOST"
      export OXIDESFU_TURN_UDP_PORT="$TURN_PORT"
      export OXIDESFU_TURN_RELAY_PORT_RANGE_START="$TURN_RELAY_MIN_PORT"
      export OXIDESFU_TURN_RELAY_PORT_RANGE_END="$TURN_RELAY_MAX_PORT"
      export OXIDESFU_TURN_ALLOW_RESTRICTED_PEER_CIDRS="$owned_allow_peer_cidrs"
      echo "Configuring OxideSFU-owned TURN on $TURN_HOST:$TURN_PORT ..."
      ;;
    coturn)
      if ! start_coturn_backend; then
        echo "failed to start coturn backend; log: $TURN_LOG_FILE" >&2
        exit 2
      fi
      ensure_turn_ice_servers_json
      ;;
    *)
      echo "unsupported OXIDESFU_DISCOVERY_TURN_BACKEND=$selected_backend (expected oxide or coturn)" >&2
      exit 2
      ;;
  esac
}

mkdir -p "$LOG_DIR"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="$LOG_DIR/server-sdk-go-full-suite-$TIMESTAMP.log"

cd "$ROOT"
configure_external_turn_if_requested
start_local_turn_if_enabled
if [[ "$REUSE_SERVER" == "true" ]]; then
  echo "Reusing existing OxideSFU at $HTTP_URL"
  wait_for_http
else
  echo "Building oxidesfu-server..."
  cargo build -p oxidesfu-server

  echo "Starting OxideSFU at $HTTP_URL..."
  cargo run -p oxidesfu-server -- \
    --bind "$HOST:$PORT" \
    --api-key "$API_KEY" \
    --api-secret "$API_SECRET" >/tmp/oxidesfu-conformance-server.log 2>&1 &
  SERVER_PID="$!"
  wait_for_http
fi

cd "$SERVER_SDK_GO"

echo "Running exploratory full server-sdk-go suite against OxideSFU..."
echo "Log: $LOG_FILE"
if [[ -n "$GO_TEST_SKIP_PATTERN" ]]; then
  echo "WARNING: running with go test skip pattern: $GO_TEST_SKIP_PATTERN"
  echo "WARNING: this is a partial discovery run (some upstream tests are intentionally skipped)"
fi

echo "=== server-sdk-go full suite discovery ===" >"$LOG_FILE"
echo "timestamp: $TIMESTAMP" >>"$LOG_FILE"
echo "http_url: $HTTP_URL" >>"$LOG_FILE"
echo "api_key: $API_KEY" >>"$LOG_FILE"
echo "reuse_server: $REUSE_SERVER" >>"$LOG_FILE"
echo "allow_failure: $ALLOW_FAILURE" >>"$LOG_FILE"
echo "go_test_extra_flags: $GO_TEST_EXTRA_FLAGS" >>"$LOG_FILE"
echo "go_test_skip_pattern: $GO_TEST_SKIP_PATTERN" >>"$LOG_FILE"
if [[ -n "$GO_TEST_SKIP_PATTERN" ]]; then
  echo "warning: partial discovery run; tests skipped by pattern: $GO_TEST_SKIP_PATTERN" >>"$LOG_FILE"
fi
echo "turn_mode: $TURN_MODE" >>"$LOG_FILE"
echo "turn_backend: $TURN_BACKEND" >>"$LOG_FILE"
echo "turn_host: $TURN_HOST" >>"$LOG_FILE"
echo "turn_port: $TURN_PORT" >>"$LOG_FILE"
echo "turn_allowed_peer_cidrs: $TURN_ALLOWED_PEER_CIDRS" >>"$LOG_FILE"
echo "external_turn_host: $EXTERNAL_TURN_HOST" >>"$LOG_FILE"
echo "external_turn_udp_port: $EXTERNAL_TURN_UDP_PORT" >>"$LOG_FILE"
echo "external_turn_tls_port: $EXTERNAL_TURN_TLS_PORT" >>"$LOG_FILE"
echo >>"$LOG_FILE"

PACKAGE_COUNT="$(go list ./... | wc -l | tr -d ' ')"
echo "Discovered $PACKAGE_COUNT Go packages in server-sdk-go" | tee -a "$LOG_FILE"
GO_TEST_COMMAND="go test ./... -count=1 -v $GO_TEST_EXTRA_FLAGS"
if [[ -n "$GO_TEST_SKIP_PATTERN" ]]; then
  GO_TEST_COMMAND="$GO_TEST_COMMAND -skip $GO_TEST_SKIP_PATTERN"
fi

echo "Running: $GO_TEST_COMMAND" | tee -a "$LOG_FILE"

set +e
LIVEKIT_URL="$HTTP_URL" \
LIVEKIT_API_KEY="$API_KEY" \
LIVEKIT_API_SECRET="$API_SECRET" \
LIVEKIT_KEYS="$API_KEY: $API_SECRET" \
$GO_TEST_COMMAND 2>&1 | tee -a "$LOG_FILE"
TEST_EXIT=${PIPESTATUS[0]}
set -e

if [[ "$TEST_EXIT" -ne 0 ]]; then
  echo
  echo "server-sdk-go full suite failed with exit code $TEST_EXIT"
  echo "Review log: $LOG_FILE"
  if [[ "$ALLOW_FAILURE" == "true" ]]; then
    echo "OXIDESFU_DISCOVERY_ALLOW_FAILURE=true, returning success for exploratory run."
    exit 0
  fi
  exit "$TEST_EXIT"
fi

echo

echo "server-sdk-go full suite passed."
echo "Log: $LOG_FILE"
