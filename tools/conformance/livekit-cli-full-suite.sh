#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LIVEKIT_CLI_REPO="${LIVEKIT_CLI_REPO:-$ROOT/../othercode/livekit-cli}"

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
SKIP_REASON="${OXIDESFU_DISCOVERY_SKIP_REASON-}"
INCLUDE_CLOUD_E2E="${OXIDESFU_DISCOVERY_LIVEKIT_CLI_INCLUDE_CLOUD_E2E:-false}"
DEFAULT_SESSION_E2E_SKIP_PATTERN="TestSessionE2E$"
DEFAULT_SESSION_E2E_SKIP_REASON="TestSessionE2E depends on external cloud LLM credentials/runtime; it is not purely OxideSFU protocol coverage in this environment"
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

if [[ ! -d "$LIVEKIT_CLI_REPO" ]]; then
  echo "livekit-cli checkout not found: $LIVEKIT_CLI_REPO" >&2
  echo "Set LIVEKIT_CLI_REPO=/path/to/livekit-cli" >&2
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
LOG_FILE="$LOG_DIR/livekit-cli-full-suite-$TIMESTAMP.log"

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

cd "$LIVEKIT_CLI_REPO"

if [[ ! -f pkg/portaudio/pa_src/include/portaudio.h ]]; then
  echo "Initializing livekit-cli submodules (PortAudio vendored sources)..."
  git submodule update --init --recursive
fi

if [[ "$INCLUDE_CLOUD_E2E" != "true" && -z "$GO_TEST_SKIP_PATTERN" ]]; then
  GO_TEST_SKIP_PATTERN="$DEFAULT_SESSION_E2E_SKIP_PATTERN"
fi
if [[ "$GO_TEST_SKIP_PATTERN" == "$DEFAULT_SESSION_E2E_SKIP_PATTERN" && -z "$SKIP_REASON" ]]; then
  SKIP_REASON="$DEFAULT_SESSION_E2E_SKIP_REASON"
fi

echo "Running exploratory livekit-cli full suite against OxideSFU..."
echo "Log: $LOG_FILE"
if [[ "$INCLUDE_CLOUD_E2E" == "true" ]]; then
  echo "NOTE: OXIDESFU_DISCOVERY_LIVEKIT_CLI_INCLUDE_CLOUD_E2E=true; running without built-in TestSessionE2E skip unless explicitly provided via OXIDESFU_DISCOVERY_GO_TEST_SKIP_PATTERN"
fi
if [[ -n "$GO_TEST_SKIP_PATTERN" ]]; then
  echo "WARNING: running with go test skip pattern: $GO_TEST_SKIP_PATTERN"
  if [[ -n "$SKIP_REASON" ]]; then
    echo "WARNING: skip reason: $SKIP_REASON"
  fi
  echo "WARNING: this is a partial discovery run (some upstream tests are intentionally skipped)"
fi

echo "=== livekit-cli full suite discovery ===" >"$LOG_FILE"
echo "timestamp: $TIMESTAMP" >>"$LOG_FILE"
echo "http_url: $HTTP_URL" >>"$LOG_FILE"
echo "api_key: $API_KEY" >>"$LOG_FILE"
echo "reuse_server: $REUSE_SERVER" >>"$LOG_FILE"
echo "allow_failure: $ALLOW_FAILURE" >>"$LOG_FILE"
echo "go_test_extra_flags: $GO_TEST_EXTRA_FLAGS" >>"$LOG_FILE"
echo "go_test_skip_pattern: $GO_TEST_SKIP_PATTERN" >>"$LOG_FILE"
echo "skip_reason: $SKIP_REASON" >>"$LOG_FILE"
echo "include_cloud_e2e: $INCLUDE_CLOUD_E2E" >>"$LOG_FILE"
if [[ -n "$GO_TEST_SKIP_PATTERN" ]]; then
  echo "warning: partial discovery run; tests skipped by pattern: $GO_TEST_SKIP_PATTERN" >>"$LOG_FILE"
  if [[ -n "$SKIP_REASON" ]]; then
    echo "warning: skip reason: $SKIP_REASON" >>"$LOG_FILE"
  fi
fi
echo >>"$LOG_FILE"

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
  echo "livekit-cli full suite failed with exit code $TEST_EXIT"
  echo "Review log: $LOG_FILE"
  if [[ "$ALLOW_FAILURE" == "true" ]]; then
    echo "OXIDESFU_DISCOVERY_ALLOW_FAILURE=true, returning success for exploratory run."
    exit 0
  fi
  exit "$TEST_EXIT"
fi

echo

echo "livekit-cli full suite passed."
echo "Log: $LOG_FILE"
