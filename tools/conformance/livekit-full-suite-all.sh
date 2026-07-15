#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LIVEKIT_REPO="${LIVEKIT_REPO:-$ROOT/../othercode/livekit}"

HOST="${OXIDESFU_HOST:-127.0.0.1}"
PORT="${OXIDESFU_PORT:-7880}"
LOCAL_HTTP_URL="http://$HOST:$PORT"
API_KEY="${LIVEKIT_API_KEY:-devkey}"
API_SECRET="${LIVEKIT_API_SECRET:-secret}"
REUSE_SERVER="${OXIDESFU_REUSE_SERVER:-false}"
ALLOW_FAILURE="${OXIDESFU_DISCOVERY_ALLOW_FAILURE:-false}"
LOG_DIR="${OXIDESFU_DISCOVERY_LOG_DIR:-$ROOT/target/conformance}"
GO_TEST_EXTRA_FLAGS="${OXIDESFU_DISCOVERY_GO_TEST_EXTRA_FLAGS:--p 8 -parallel 8}"
GO_TEST_SHARD_EXTRA_FLAGS="${OXIDESFU_DISCOVERY_GO_TEST_SHARD_EXTRA_FLAGS:--p 1 -parallel 1}"
GO_TEST_SKIP_PATTERN="${OXIDESFU_DISCOVERY_GO_TEST_SKIP_PATTERN-}"
GO_TEST_RUN_PATTERN="${OXIDESFU_DISCOVERY_GO_TEST_RUN_PATTERN-}"
SKIP_REASON="${OXIDESFU_DISCOVERY_SKIP_REASON-}"
INCLUDE_UPSTREAM_INTEGRATION="${OXIDESFU_DISCOVERY_LIVEKIT_INCLUDE_UPSTREAM_INTEGRATION:-true}"
PACKAGE_SCOPE="${OXIDESFU_DISCOVERY_LIVEKIT_PACKAGE_SCOPE:-external}"
SHARD_TESTS="${OXIDESFU_DISCOVERY_LIVEKIT_SHARD_TESTS:-true}"
SHARD_WORKERS="${OXIDESFU_DISCOVERY_LIVEKIT_SHARD_WORKERS:-4}"
SHARD_BASE_PORT="${OXIDESFU_DISCOVERY_LIVEKIT_SHARD_BASE_PORT:-18000}"
SHARD_PORT_STRIDE="${OXIDESFU_DISCOVERY_LIVEKIT_SHARD_PORT_STRIDE:-100}"
SKIP_EGRESS_STORE="${OXIDESFU_DISCOVERY_LIVEKIT_SKIP_EGRESS_STORE:-false}"
LIVEKIT_EXPECT_BRANCH="${OXIDESFU_DISCOVERY_LIVEKIT_EXPECT_BRANCH:-oxidesfu/livekit-compat}"
LIVEKIT_TEST_SERVER_PORT="${OXIDESFU_DISCOVERY_LIVEKIT_TEST_SERVER_PORT:-17880}"
LIVEKIT_TEST_SERVER_PORT_SECOND="${OXIDESFU_DISCOVERY_LIVEKIT_TEST_SERVER_PORT_SECOND:-18880}"
LIVEKIT_TEST_TURN_UDP_PORT="${OXIDESFU_DISCOVERY_LIVEKIT_TEST_TURN_UDP_PORT:-19478}"
LIVEKIT_TEST_WEBHOOK_PORT="${OXIDESFU_DISCOVERY_LIVEKIT_TEST_WEBHOOK_PORT:-17890}"
LIVEKIT_TEST_TURN_RESTRICTED_PEER_CIDRS="${OXIDESFU_DISCOVERY_LIVEKIT_TURN_RESTRICTED_PEER_CIDRS:-10.0.0.0/8,172.16.0.0/12,192.168.0.0/16}"
DATACHANNEL_SLOW_THRESHOLD="${OXIDESFU_DISCOVERY_DATACHANNEL_SLOW_THRESHOLD:-0}"
PARTICIPANT_DATA_BLOB_ENABLED="${OXIDESFU_DISCOVERY_PARTICIPANT_DATA_BLOB_ENABLED:-true}"
ROOM_AUTO_CREATE="${OXIDESFU_DISCOVERY_ROOM_AUTO_CREATE:-true}"
SERVER_PID=""
SERVER_BIN="$ROOT/target/debug/oxidesfu-server"

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

if [[ ! -d "$LIVEKIT_REPO" ]]; then
  echo "livekit checkout not found: $LIVEKIT_REPO" >&2
  echo "Set LIVEKIT_REPO=/path/to/livekit" >&2
  exit 2
fi

wait_for_http_url() {
  local url="$1"
  local deadline
  deadline=$((SECONDS + 45))
  while (( SECONDS < deadline )); do
    local status
    status="$(curl -sS -o /dev/null -w '%{http_code}' "$url/rtc/validate" 2>/dev/null || true)"
    if [[ "$status" != "000" ]]; then
      return 0
    fi
    sleep 0.25
  done
  echo "OxideSFU did not become reachable at $url within 45s" >&2
  return 1
}

wait_for_http() {
  wait_for_http_url "$HTTP_URL"
}

sanitize_test_name() {
  printf '%s' "$1" | tr -c 'A-Za-z0-9_.-' '_'
}

write_log_header() {
  mkdir -p "$LOG_DIR"
  {
    echo "=== livekit full suite discovery ==="
    echo "timestamp: $TIMESTAMP"
    echo "http_url: $HTTP_URL"
    echo "api_key: $API_KEY"
    echo "reuse_server: $REUSE_SERVER"
    echo "allow_failure: $ALLOW_FAILURE"
    echo "go_test_extra_flags: $GO_TEST_EXTRA_FLAGS"
    echo "go_test_shard_extra_flags: $GO_TEST_SHARD_EXTRA_FLAGS"
    echo "go_test_skip_pattern: $GO_TEST_SKIP_PATTERN"
    echo "go_test_run_pattern: $GO_TEST_RUN_PATTERN"
    echo "skip_reason: $SKIP_REASON"
    echo "include_upstream_integration: $INCLUDE_UPSTREAM_INTEGRATION"
    echo "package_scope: $PACKAGE_SCOPE"
    echo "shard_tests: $SHARD_TESTS"
    echo "shard_workers: $SHARD_WORKERS"
    echo "shard_base_port: $SHARD_BASE_PORT"
    echo "shard_port_stride: $SHARD_PORT_STRIDE"
    echo "skip_egress_store: $SKIP_EGRESS_STORE"
    echo "livekit_expect_branch: $LIVEKIT_EXPECT_BRANCH"
    echo "livekit_test_server_port: $LIVEKIT_TEST_SERVER_PORT"
    echo "livekit_test_server_port_second: $LIVEKIT_TEST_SERVER_PORT_SECOND"
    echo "livekit_test_turn_udp_port: $LIVEKIT_TEST_TURN_UDP_PORT"
    echo "livekit_test_webhook_port: $LIVEKIT_TEST_WEBHOOK_PORT"
    echo "livekit_test_turn_restricted_peer_cidrs: $LIVEKIT_TEST_TURN_RESTRICTED_PEER_CIDRS"
    echo "datachannel_slow_threshold: $DATACHANNEL_SLOW_THRESHOLD"
    echo "participant_data_blob_enabled: $PARTICIPANT_DATA_BLOB_ENABLED"
    if [[ -n "$GO_TEST_SKIP_PATTERN" ]]; then
      echo "warning: partial discovery run; tests skipped by pattern: $GO_TEST_SKIP_PATTERN"
      if [[ -n "$SKIP_REASON" ]]; then
        echo "warning: skip reason: $SKIP_REASON"
      fi
    fi
    echo
  } >"$LOG_FILE"
}

start_global_server_if_needed() {
  if [[ "$REUSE_SERVER" == "true" ]]; then
    echo "Reusing existing OxideSFU at $HTTP_URL"
    wait_for_http
    return 0
  fi

  echo "Building oxidesfu-server..."
  (cd "$ROOT" && cargo build -p oxidesfu-server)

  echo "Starting OxideSFU at $HTTP_URL..."
  OXIDESFU_BIND="$HOST:$PORT" \
  OXIDESFU_API_KEY="$API_KEY" \
  OXIDESFU_API_SECRET="$API_SECRET" \
  OXIDESFU_DATACHANNEL_SLOW_THRESHOLD="$DATACHANNEL_SLOW_THRESHOLD" \
  OXIDESFU_PARTICIPANT_DATA_BLOB_ENABLED="$PARTICIPANT_DATA_BLOB_ENABLED" \
  OXIDESFU_WEBHOOK_API_KEY="$API_KEY" \
  OXIDESFU_WEBHOOK_URLS="http://$HOST:$LIVEKIT_TEST_WEBHOOK_PORT" \
  "$SERVER_BIN" >/tmp/oxidesfu-conformance-server.log 2>&1 &
  SERVER_PID="$!"
  wait_for_http
}

build_server_if_needed() {
  if [[ "$REUSE_SERVER" == "true" ]]; then
    return 0
  fi
  echo "Building oxidesfu-server..."
  (cd "$ROOT" && cargo build -p oxidesfu-server)
}

run_non_test_packages_if_requested() {
  if [[ "$PACKAGE_SCOPE" != "all" ]]; then
    return 0
  fi

  mapfile -t ALL_PACKAGES < <(go list ./...)
  NON_TEST_PACKAGES=()
  for package in "${ALL_PACKAGES[@]}"; do
    if [[ "$package" == "github.com/livekit/livekit-server/test" ]]; then
      continue
    fi
    NON_TEST_PACKAGES+=("$package")
  done

  if [[ "${#NON_TEST_PACKAGES[@]}" -eq 0 ]]; then
    return 0
  fi

  echo "Running ${#NON_TEST_PACKAGES[@]} non-./test LiveKit packages with Go package parallelism..." | tee -a "$LOG_FILE"
  local cmd="go test ${NON_TEST_PACKAGES[*]} -count=1 -v $GO_TEST_EXTRA_FLAGS"
  echo "Running: $cmd" | tee -a "$LOG_FILE"
  set +e
  $cmd 2>&1 | tee -a "$LOG_FILE"
  local exit_code=${PIPESTATUS[0]}
  set -e
  return "$exit_code"
}

list_livekit_test_names() {
  local list_pattern="$GO_TEST_RUN_PATTERN"
  if [[ -z "$list_pattern" ]]; then
    list_pattern='^Test'
  fi
  go test ./test -list "$list_pattern" 2>/dev/null | grep '^Test' || true
}

run_one_shard() {
  local test_name="$1"
  local index="$2"
  local safe_name
  safe_name="$(sanitize_test_name "$test_name")"
  local shard_port=$((SHARD_BASE_PORT + index * SHARD_PORT_STRIDE))
  local shard_http_url="http://$HOST:$shard_port"
  local shard_tcp_port=$((shard_port + 1))
  local shard_udp_start=$((shard_port + 10))
  local shard_udp_end=$((shard_port + 39))
  local shard_webhook_port=$((shard_port + 50))
  local shard_webhook_url="http://$HOST:$shard_webhook_port"
  local shard_turn_enabled="false"
  local shard_turn_domain=""
  local shard_turn_udp_port=""
  local shard_test_turn_udp_port=$((shard_port + 70))
  local shard_turn_allow_cidrs=""
  local shard_turn_deny_cidrs=""
  local local_turn_peer_cidr=""
  if [[ "$HOST" == "127.0.0.1" ]]; then
    # webrtc-rs reports its wildcard UDP bind as the related address in this
    # local topology; both it and loopback are restricted TURN peers.
    local_turn_peer_cidr="127.0.0.1/32,0.0.0.0/32"
  elif [[ "$HOST" == "::1" ]]; then
    local_turn_peer_cidr="::1/128,::/128"
  fi
  local shard_dir="$LOG_DIR/livekit-shards-$TIMESTAMP/$safe_name"
  local server_log="$shard_dir/oxidesfu-server.log"
  local test_log="$shard_dir/go-test.log"
  local status_file="$shard_dir/status"
  local server_pid=""
  local shard_datachannel_slow_threshold="$DATACHANNEL_SLOW_THRESHOLD"
  local shard_participant_data_blob_enabled="$PARTICIPANT_DATA_BLOB_ENABLED"
  local shard_room_auto_create="$ROOM_AUTO_CREATE"

  if [[ "$test_name" == "TestDataPublishSlowSubscriber" && "$shard_datachannel_slow_threshold" == "0" ]]; then
    shard_datachannel_slow_threshold="21024"
  fi
  if [[ "$test_name" == "TestSingleNodeDataBlobDisabled" ]]; then
    shard_participant_data_blob_enabled="false"
  fi
  if [[ "$test_name" == "TestAutoCreate" ]]; then
    shard_room_auto_create="false"
  fi
  if [[ "$test_name" == "TestTurnAuthFailure" ]]; then
    shard_turn_enabled="true"
  fi
  case "$test_name" in
    TestTurnRelay/allow)
      shard_turn_enabled="true"
      shard_turn_allow_cidrs="$LIVEKIT_TEST_TURN_RESTRICTED_PEER_CIDRS"
      if [[ -n "$local_turn_peer_cidr" ]]; then
        shard_turn_allow_cidrs="$shard_turn_allow_cidrs,$local_turn_peer_cidr"
      fi
      ;;
    TestTurnRelay/not-allowed)
      shard_turn_enabled="true"
      ;;
    TestTurnRelay/denied-overrides-allowed)
      shard_turn_enabled="true"
      shard_turn_allow_cidrs="$LIVEKIT_TEST_TURN_RESTRICTED_PEER_CIDRS"
      shard_turn_deny_cidrs="$LIVEKIT_TEST_TURN_RESTRICTED_PEER_CIDRS"
      if [[ -n "$local_turn_peer_cidr" ]]; then
        shard_turn_allow_cidrs="$shard_turn_allow_cidrs,$local_turn_peer_cidr"
        shard_turn_deny_cidrs="$shard_turn_deny_cidrs,$local_turn_peer_cidr"
      fi
      ;;
  esac

  if [[ "$shard_turn_enabled" == "true" ]]; then
    shard_turn_domain="$HOST"
    shard_turn_udp_port="$shard_test_turn_udp_port"
  fi

  mkdir -p "$shard_dir"

  {
    echo "=== shard $index: $test_name ==="
    echo "http_url: $shard_http_url"
    echo "rtc_tcp_port: $shard_tcp_port"
    echo "rtc_udp_range: $shard_udp_start-$shard_udp_end"
    echo "webhook_url: $shard_webhook_url"
    echo "turn_udp_port: ${shard_turn_udp_port:-disabled}"
    echo "test_turn_udp_port: $shard_test_turn_udp_port"
    echo "turn_enabled: $shard_turn_enabled"
    echo "turn_domain: ${shard_turn_domain:-disabled}"
    echo "turn_allow_restricted_peer_cidrs: $shard_turn_allow_cidrs"
    echo "turn_deny_peer_cidrs: $shard_turn_deny_cidrs"
    echo "datachannel_slow_threshold: $shard_datachannel_slow_threshold"
    echo "participant_data_blob_enabled: $shard_participant_data_blob_enabled"
    echo "room_auto_create: $shard_room_auto_create"
    echo "test_log: $test_log"
    echo "server_log: $server_log"
  } >"$shard_dir/metadata.log"

  if [[ "$REUSE_SERVER" == "true" ]]; then
    shard_http_url="$HTTP_URL"
  else
    local -a server_env=(
      "OXIDESFU_BIND=$HOST:$shard_port"
      "OXIDESFU_API_KEY=$API_KEY"
      "OXIDESFU_API_SECRET=$API_SECRET"
      "OXIDESFU_RTC_TCP_PORT=$shard_tcp_port"
      "OXIDESFU_RTC_UDP_PORT_RANGE_START=$shard_udp_start"
      "OXIDESFU_RTC_UDP_PORT_RANGE_END=$shard_udp_end"
      "OXIDESFU_TURN_ENABLED=$shard_turn_enabled"
      "OXIDESFU_DATACHANNEL_SLOW_THRESHOLD=$shard_datachannel_slow_threshold"
      "OXIDESFU_PARTICIPANT_DATA_BLOB_ENABLED=$shard_participant_data_blob_enabled"
      "OXIDESFU_ROOM_AUTO_CREATE=$shard_room_auto_create"
      "OXIDESFU_WEBHOOK_API_KEY=$API_KEY"
      "OXIDESFU_WEBHOOK_URLS=$shard_webhook_url"
    )
    if [[ "$shard_turn_enabled" == "true" ]]; then
      server_env+=(
        "OXIDESFU_TURN_DOMAIN=$shard_turn_domain"
        "OXIDESFU_TURN_BIND=$HOST"
        "OXIDESFU_TURN_UDP_PORT=$shard_turn_udp_port"
        "OXIDESFU_TURN_ALLOW_RESTRICTED_PEER_CIDRS=$shard_turn_allow_cidrs"
        "OXIDESFU_TURN_DENY_PEER_CIDRS=$shard_turn_deny_cidrs"
      )
    fi
    env \
      -u OXIDESFU_TURN_DOMAIN \
      -u OXIDESFU_TURN_BIND \
      -u OXIDESFU_TURN_UDP_PORT \
      -u OXIDESFU_TURN_TLS_PORT \
      -u OXIDESFU_TURN_ALLOW_RESTRICTED_PEER_CIDRS \
      -u OXIDESFU_TURN_DENY_PEER_CIDRS \
      "${server_env[@]}" \
      "$SERVER_BIN" >"$server_log" 2>&1 &
    server_pid="$!"
    if ! wait_for_http_url "$shard_http_url" >>"$server_log" 2>&1; then
      echo "SERVER_START_FAILED" >"$status_file"
      if [[ -n "$server_pid" ]]; then
        kill "$server_pid" >/dev/null 2>&1 || true
        wait "$server_pid" >/dev/null 2>&1 || true
      fi
      return 1
    fi
  fi

  set +e
  if [[ -n "$GO_TEST_SKIP_PATTERN" ]]; then
    LK_EXTERNAL_SERVER_URL="$shard_http_url" \
    LIVEKIT_URL="$shard_http_url" \
    LIVEKIT_API_KEY="$API_KEY" \
    LIVEKIT_API_SECRET="$API_SECRET" \
    LIVEKIT_KEYS="$API_KEY: $API_SECRET" \
    LK_TEST_SERVER_PORT="$LIVEKIT_TEST_SERVER_PORT" \
    LK_TEST_SERVER_PORT_SECOND="$LIVEKIT_TEST_SERVER_PORT_SECOND" \
    LK_TEST_TURN_UDP_PORT="$shard_test_turn_udp_port" \
    LK_TEST_WEBHOOK_PORT="$shard_webhook_port" \
    LK_TEST_TURN_RESTRICTED_PEER_CIDRS="$LIVEKIT_TEST_TURN_RESTRICTED_PEER_CIDRS" \
    LK_EXTERNAL_DATACHANNEL_SLOW_THRESHOLD="$shard_datachannel_slow_threshold" \
    go test ./test -run "^${test_name}$" -count=1 -v $GO_TEST_SHARD_EXTRA_FLAGS -skip "$GO_TEST_SKIP_PATTERN" >"$test_log" 2>&1
  else
    LK_EXTERNAL_SERVER_URL="$shard_http_url" \
    LIVEKIT_URL="$shard_http_url" \
    LIVEKIT_API_KEY="$API_KEY" \
    LIVEKIT_API_SECRET="$API_SECRET" \
    LIVEKIT_KEYS="$API_KEY: $API_SECRET" \
    LK_TEST_SERVER_PORT="$LIVEKIT_TEST_SERVER_PORT" \
    LK_TEST_SERVER_PORT_SECOND="$LIVEKIT_TEST_SERVER_PORT_SECOND" \
    LK_TEST_TURN_UDP_PORT="$shard_test_turn_udp_port" \
    LK_TEST_WEBHOOK_PORT="$shard_webhook_port" \
    LK_TEST_TURN_RESTRICTED_PEER_CIDRS="$LIVEKIT_TEST_TURN_RESTRICTED_PEER_CIDRS" \
    LK_EXTERNAL_DATACHANNEL_SLOW_THRESHOLD="$shard_datachannel_slow_threshold" \
    go test ./test -run "^${test_name}$" -count=1 -v $GO_TEST_SHARD_EXTRA_FLAGS >"$test_log" 2>&1
  fi
  local test_exit=$?
  set -e

  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" >/dev/null 2>&1 || true
    wait "$server_pid" >/dev/null 2>&1 || true
  fi

  if [[ "$test_exit" -eq 0 ]]; then
    if grep -Eq '^--- SKIP:' "$test_log" || grep -Eq 'no tests to run' "$test_log"; then
      echo "SKIP" >"$status_file"
      echo "SKIP $test_name (log: $test_log)" | tee -a "$LOG_FILE"
      return 1
    fi

    echo "PASS" >"$status_file"
    echo "PASS $test_name" | tee -a "$LOG_FILE"
    return 0
  fi

  echo "FAIL" >"$status_file"
  echo "FAIL $test_name (log: $test_log)" | tee -a "$LOG_FILE"
  return "$test_exit"
}

run_sharded_livekit_test_package() {
  if [[ "$INCLUDE_UPSTREAM_INTEGRATION" != "true" ]]; then
    echo "WARNING: upstream ./test package excluded by OXIDESFU_DISCOVERY_LIVEKIT_INCLUDE_UPSTREAM_INTEGRATION=false" | tee -a "$LOG_FILE"
    return 0
  fi

  mapfile -t TEST_NAMES < <(list_livekit_test_names)
  # External-server mode cannot mutate an already-running OxideSFU instance
  # between upstream subtests. Split the three policy cases into independent
  # server shards so each listener owns the exact CIDR configuration it tests.
  local expanded_test_names=()
  for test_name in "${TEST_NAMES[@]}"; do
    if [[ "$test_name" == "TestTurnRelay" ]]; then
      expanded_test_names+=("TestTurnRelay/allow" "TestTurnRelay/not-allowed" "TestTurnRelay/denied-overrides-allowed")
    else
      expanded_test_names+=("$test_name")
    fi
  done
  TEST_NAMES=("${expanded_test_names[@]}")
  if [[ "${#TEST_NAMES[@]}" -eq 0 ]]; then
    echo "No LiveKit ./test tests matched pattern '${GO_TEST_RUN_PATTERN:-^Test}'." | tee -a "$LOG_FILE"
    return 0
  fi

  if [[ "$REUSE_SERVER" == "true" ]]; then
    wait_for_http
  else
    build_server_if_needed
  fi

  echo "Running ${#TEST_NAMES[@]} LiveKit ./test tests as isolated OxideSFU shards with $SHARD_WORKERS workers..." | tee -a "$LOG_FILE"
  mkdir -p "$LOG_DIR/livekit-shards-$TIMESTAMP"

  local active=0
  local index=0
  local failed=0
  declare -a pids=()

  for test_name in "${TEST_NAMES[@]}"; do
    run_one_shard "$test_name" "$index" &
    pids+=("$!")
    active=$((active + 1))
    index=$((index + 1))

    if (( active >= SHARD_WORKERS )); then
      if ! wait -n; then
        failed=1
      fi
      active=$((active - 1))
    fi
  done

  while (( active > 0 )); do
    if ! wait -n; then
      failed=1
    fi
    active=$((active - 1))
  done

  echo >>"$LOG_FILE"
  echo "Shard logs: $LOG_DIR/livekit-shards-$TIMESTAMP" | tee -a "$LOG_FILE"
  echo "Shard summary:" | tee -a "$LOG_FILE"
  find "$LOG_DIR/livekit-shards-$TIMESTAMP" -name status -print | sort | while read -r status; do
    printf '  %s: %s\n' "$(basename "$(dirname "$status")")" "$(cat "$status")" | tee -a "$LOG_FILE"
  done

  return "$failed"
}

run_unsharded_livekit_test_package() {
  start_global_server_if_needed
  mapfile -t LIVEKIT_PACKAGES < <(go list ./test)
  local package_args="${LIVEKIT_PACKAGES[*]}"
  local cmd="go test $package_args -count=1 -v $GO_TEST_EXTRA_FLAGS"
  if [[ -n "$GO_TEST_RUN_PATTERN" ]]; then
    cmd="$cmd -run $GO_TEST_RUN_PATTERN"
  fi
  if [[ -n "$GO_TEST_SKIP_PATTERN" ]]; then
    cmd="$cmd -skip $GO_TEST_SKIP_PATTERN"
  fi

  echo "Running: $cmd" | tee -a "$LOG_FILE"
  set +e
  LK_EXTERNAL_SERVER_URL="$HTTP_URL" \
  LIVEKIT_URL="$HTTP_URL" \
  LIVEKIT_API_KEY="$API_KEY" \
  LIVEKIT_API_SECRET="$API_SECRET" \
  LIVEKIT_KEYS="$API_KEY: $API_SECRET" \
  LK_TEST_SERVER_PORT="$LIVEKIT_TEST_SERVER_PORT" \
  LK_TEST_SERVER_PORT_SECOND="$LIVEKIT_TEST_SERVER_PORT_SECOND" \
  LK_TEST_TURN_UDP_PORT="$LIVEKIT_TEST_TURN_UDP_PORT" \
  LK_TEST_WEBHOOK_PORT="$LIVEKIT_TEST_WEBHOOK_PORT" \
  LK_TEST_TURN_RESTRICTED_PEER_CIDRS="$LIVEKIT_TEST_TURN_RESTRICTED_PEER_CIDRS" \
  LK_EXTERNAL_DATACHANNEL_SLOW_THRESHOLD="$DATACHANNEL_SLOW_THRESHOLD" \
  $cmd 2>&1 | tee -a "$LOG_FILE"
  local exit_code=${PIPESTATUS[0]}
  set -e
  return "$exit_code"
}

mkdir -p "$LOG_DIR"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="$LOG_DIR/livekit-full-suite-$TIMESTAMP.log"

cd "$LIVEKIT_REPO"

if [[ -n "$LIVEKIT_EXPECT_BRANCH" ]]; then
  CURRENT_LIVEKIT_BRANCH="$(git --no-pager branch --show-current)"
  if [[ "$CURRENT_LIVEKIT_BRANCH" != "$LIVEKIT_EXPECT_BRANCH" ]]; then
    echo "warning: livekit repo is on branch '$CURRENT_LIVEKIT_BRANCH' but '$LIVEKIT_EXPECT_BRANCH' was expected for full-suite-all" >&2
  fi
fi

if [[ "$INCLUDE_UPSTREAM_INTEGRATION" != "true" && -z "$SKIP_REASON" ]]; then
  SKIP_REASON="excluding upstream livekit-server/test package by request"
fi

if [[ "$SKIP_EGRESS_STORE" == "true" ]]; then
  EGRESS_STORE_SKIP_PATTERN="^TestEgressStore$"
  if [[ -z "$GO_TEST_SKIP_PATTERN" ]]; then
    GO_TEST_SKIP_PATTERN="$EGRESS_STORE_SKIP_PATTERN"
  else
    GO_TEST_SKIP_PATTERN="$GO_TEST_SKIP_PATTERN|$EGRESS_STORE_SKIP_PATTERN"
  fi
fi

write_log_header

echo "Running LiveKit suite with OxideSFU external-server mode..."
echo "Log: $LOG_FILE"
echo "Package scope: $PACKAGE_SCOPE"
echo "Shard tests: $SHARD_TESTS"

set +e
run_non_test_packages_if_requested
NON_TEST_EXIT=$?
if [[ "$SHARD_TESTS" == "true" ]]; then
  run_sharded_livekit_test_package
  TEST_EXIT=$?
else
  run_unsharded_livekit_test_package
  TEST_EXIT=$?
fi
set -e

FINAL_EXIT=0
if [[ "$NON_TEST_EXIT" -ne 0 || "$TEST_EXIT" -ne 0 ]]; then
  FINAL_EXIT=1
fi

if [[ "$FINAL_EXIT" -ne 0 ]]; then
  echo
  echo "livekit suite failed"
  echo "Review log: $LOG_FILE"
  if [[ "$ALLOW_FAILURE" == "true" ]]; then
    echo "OXIDESFU_DISCOVERY_ALLOW_FAILURE=true, returning success for exploratory run."
    exit 0
  fi
  exit "$FINAL_EXIT"
fi

echo
echo "livekit suite passed."
echo "Log: $LOG_FILE"
