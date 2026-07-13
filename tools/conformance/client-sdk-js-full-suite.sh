#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CLIENT_SDK_JS_REPO="${CLIENT_SDK_JS_REPO:-$ROOT/../othercode/client-sdk-js}"
LIVEKIT_CLI_REPO="${LIVEKIT_CLI_REPO:-$ROOT/../othercode/livekit-cli}"

HOST="${OXIDESFU_HOST:-127.0.0.1}"
PORT="${OXIDESFU_PORT:-7880}"
LOCAL_HTTP_URL="http://$HOST:$PORT"
LOCAL_WS_URL="ws://$HOST:$PORT"
API_KEY="${LIVEKIT_API_KEY:-devkey}"
API_SECRET="${LIVEKIT_API_SECRET:-secret}"
REUSE_SERVER="${OXIDESFU_REUSE_SERVER:-false}"
ALLOW_FAILURE="${OXIDESFU_DISCOVERY_ALLOW_FAILURE:-false}"
LOG_DIR="${OXIDESFU_DISCOVERY_LOG_DIR:-$ROOT/target/conformance}"
LK_BIN="${LK_BIN:-}"
SERVER_PID=""

if [[ "$REUSE_SERVER" == "true" ]]; then
  HTTP_URL="${LIVEKIT_URL:-$LOCAL_HTTP_URL}"
  WS_URL="${LIVEKIT_WS_URL:-${LIVEKIT_URL:-$LOCAL_WS_URL}}"
else
  HTTP_URL="$LOCAL_HTTP_URL"
  WS_URL="$LOCAL_WS_URL"
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

if [[ ! -d "$CLIENT_SDK_JS_REPO" ]]; then
  echo "client-sdk-js checkout not found: $CLIENT_SDK_JS_REPO" >&2
  echo "Set CLIENT_SDK_JS_REPO=/path/to/client-sdk-js" >&2
  exit 2
fi

if [[ ! -d "$LIVEKIT_CLI_REPO" ]]; then
  echo "livekit-cli checkout not found: $LIVEKIT_CLI_REPO" >&2
  echo "Set LIVEKIT_CLI_REPO=/path/to/livekit-cli" >&2
  exit 2
fi

PNPM_BIN=""
if command -v pnpm >/dev/null 2>&1; then
  PNPM_BIN="pnpm"
elif command -v corepack >/dev/null 2>&1; then
  echo "pnpm not found on PATH; using corepack-backed pnpm shim..."
  SHIM_DIR="/tmp/oxidesfu-conformance-pnpm-shim"
  mkdir -p "$SHIM_DIR"
  cat >"$SHIM_DIR/pnpm" <<'EOF'
#!/usr/bin/env sh
exec corepack pnpm "$@"
EOF
  chmod +x "$SHIM_DIR/pnpm"
  export PATH="$SHIM_DIR:$PATH"
  PNPM_BIN="pnpm"
else
  echo "pnpm is required (client-sdk-js uses pnpm workspaces). Install pnpm or enable corepack." >&2
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
LOG_FILE="$LOG_DIR/client-sdk-js-full-suite-$TIMESTAMP.log"

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

if [[ -z "$LK_BIN" ]]; then
  cd "$LIVEKIT_CLI_REPO"
  if [[ ! -f pkg/portaudio/pa_src/include/portaudio.h ]]; then
    echo "Initializing livekit-cli submodules (PortAudio vendored sources)..."
    git submodule update --init --recursive
  fi
  mkdir -p /tmp/oxidesfu-conformance-bin
  LK_BIN="/tmp/oxidesfu-conformance-bin/lk"
  echo "Building livekit-cli lk binary..."
  go build -o "$LK_BIN" ./cmd/lk
fi

ROOM="client-sdk-js-discovery-$(date +%s)-$$"
IDENTITY="client-sdk-js-discovery-participant"
TOKEN="$($LK_BIN \
  --url "$HTTP_URL" \
  --api-key "$API_KEY" \
  --api-secret "$API_SECRET" \
  --yes \
  token create \
  --room "$ROOM" \
  --identity "$IDENTITY" \
  --join \
  --valid-for 30m \
  --token-only)"

cd "$CLIENT_SDK_JS_REPO"

echo "=== client-sdk-js full suite discovery ===" >"$LOG_FILE"
echo "timestamp: $TIMESTAMP" >>"$LOG_FILE"
echo "http_url: $HTTP_URL" >>"$LOG_FILE"
echo "ws_url: $WS_URL" >>"$LOG_FILE"
echo "api_key: $API_KEY" >>"$LOG_FILE"
echo "reuse_server: $REUSE_SERVER" >>"$LOG_FILE"
echo "allow_failure: $ALLOW_FAILURE" >>"$LOG_FILE"
echo >>"$LOG_FILE"

set +e
{
  # Re-enable fail-fast inside the grouped pipeline so any failed step
  # (especially pnpm test) makes the whole discovery run fail.
  set -e

  export CI=true

  echo "Installing dependencies..."
  $PNPM_BIN install --frozen-lockfile

  echo "Running root unit tests (pnpm test)..."
  $PNPM_BIN test

  echo "Building livekit-client dist package..."
  $PNPM_BIN build

  echo "Preparing smoke test package tarball..."
  rm -f smoke-tests/livekit-client.tgz smoke-tests/livekit-client-*.tgz
  npm pack --pack-destination smoke-tests >/dev/null
  PACKED_TGZ="$(ls smoke-tests/livekit-client-*.tgz | head -n1)"
  mv "$PACKED_TGZ" smoke-tests/livekit-client.tgz

  echo "Installing smoke-tests dependencies..."
  npm --prefix smoke-tests install

  echo "Building smoke test fixtures..."
  npm --prefix smoke-tests run pretest

  echo "Ensuring Playwright Chromium is installed..."
  npx --prefix smoke-tests playwright install chromium

  echo "Running smoke-tests playwright suite..."
  LIVEKIT_TEST_URL="$WS_URL" \
  LIVEKIT_TEST_TOKEN="$TOKEN" \
  npx --prefix smoke-tests playwright test --config smoke-tests/playwright.config.ts --reporter=list
} 2>&1 | tee -a "$LOG_FILE"
TEST_EXIT=${PIPESTATUS[0]}
set -e

if [[ "$TEST_EXIT" -ne 0 ]]; then
  echo
  echo "client-sdk-js full suite failed with exit code $TEST_EXIT"
  echo "Review log: $LOG_FILE"
  if [[ "$ALLOW_FAILURE" == "true" ]]; then
    echo "OXIDESFU_DISCOVERY_ALLOW_FAILURE=true, returning success for exploratory run."
    exit 0
  fi
  exit "$TEST_EXIT"
fi

echo

echo "client-sdk-js full suite passed."
echo "Log: $LOG_FILE"
