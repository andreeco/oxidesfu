#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUST_SDKS_REPO="${RUST_SDKS_REPO:-$ROOT/../othercode/rust-sdks}"

HOST="${OXIDESFU_HOST:-127.0.0.1}"
PORT="${OXIDESFU_PORT:-7880}"
LOCAL_HTTP_URL="http://$HOST:$PORT"
API_KEY="${LIVEKIT_API_KEY:-devkey}"
API_SECRET="${LIVEKIT_API_SECRET:-secret}"
REUSE_SERVER="${OXIDESFU_REUSE_SERVER:-false}"
ALLOW_FAILURE="${OXIDESFU_DISCOVERY_ALLOW_FAILURE:-false}"
LOG_DIR="${OXIDESFU_DISCOVERY_LOG_DIR:-$ROOT/target/conformance}"
RUST_TEST_FILTER="${OXIDESFU_DISCOVERY_RUST_TEST_FILTER-}"
CARGO_TEST_EXTRA_ARGS="${OXIDESFU_DISCOVERY_RUST_SDKS_CARGO_TEST_EXTRA_ARGS:-}"
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
}
trap cleanup EXIT

if [[ ! -d "$RUST_SDKS_REPO" ]]; then
  echo "rust-sdks checkout not found: $RUST_SDKS_REPO" >&2
  echo "Set RUST_SDKS_REPO=/path/to/rust-sdks" >&2
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

mkdir -p "$LOG_DIR"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="$LOG_DIR/rust-sdks-full-suite-$TIMESTAMP.log"

cd "$ROOT"
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

cd "$RUST_SDKS_REPO"

echo "Running exploratory rust-sdks full suite against OxideSFU..."
echo "Log: $LOG_FILE"

echo "=== rust-sdks full suite discovery ===" >"$LOG_FILE"
echo "timestamp: $TIMESTAMP" >>"$LOG_FILE"
echo "http_url: $HTTP_URL" >>"$LOG_FILE"
echo "api_key: $API_KEY" >>"$LOG_FILE"
echo "reuse_server: $REUSE_SERVER" >>"$LOG_FILE"
echo "allow_failure: $ALLOW_FAILURE" >>"$LOG_FILE"
echo "rust_test_filter: $RUST_TEST_FILTER" >>"$LOG_FILE"
echo "cargo_test_extra_args: $CARGO_TEST_EXTRA_ARGS" >>"$LOG_FILE"
echo >>"$LOG_FILE"

CARGO_TEST_COMMAND="cargo test --workspace --all-targets"
if [[ -n "$RUST_TEST_FILTER" ]]; then
  CARGO_TEST_COMMAND="$CARGO_TEST_COMMAND $RUST_TEST_FILTER"
fi
if [[ -n "$CARGO_TEST_EXTRA_ARGS" ]]; then
  CARGO_TEST_COMMAND="$CARGO_TEST_COMMAND $CARGO_TEST_EXTRA_ARGS"
fi
CARGO_TEST_COMMAND="$CARGO_TEST_COMMAND -- --nocapture"

echo "Running: $CARGO_TEST_COMMAND" | tee -a "$LOG_FILE"

set +e
LIVEKIT_URL="$HTTP_URL" \
LIVEKIT_API_KEY="$API_KEY" \
LIVEKIT_API_SECRET="$API_SECRET" \
$CARGO_TEST_COMMAND 2>&1 | tee -a "$LOG_FILE"
TEST_EXIT=${PIPESTATUS[0]}
set -e

if [[ "$TEST_EXIT" -ne 0 ]]; then
  echo
  echo "rust-sdks full suite failed with exit code $TEST_EXIT"
  echo "Review log: $LOG_FILE"
  if [[ "$ALLOW_FAILURE" == "true" ]]; then
    echo "OXIDESFU_DISCOVERY_ALLOW_FAILURE=true, returning success for exploratory run."
    exit 0
  fi
  exit "$TEST_EXIT"
fi

echo

echo "rust-sdks full suite passed."
echo "Log: $LOG_FILE"
