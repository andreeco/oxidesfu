#!/usr/bin/env bash
# Script-level contract tests for profile-load-test.sh option handling.
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly RUNNER="${SCRIPT_DIR}/profile-load-test.sh"

fail() {
    echo "test failure: $*" >&2
    exit 1
}

assert_contains() {
    local output="$1"
    local expected="$2"
    [[ "${output}" == *"${expected}"* ]] || fail "expected output to contain: ${expected}\nactual: ${output}"
}

assert_fails_with() {
    local expected="$1"
    shift
    local output
    if output="$("$@" 2>&1)"; then
        fail "expected command to fail: $*"
    fi
    assert_contains "${output}" "${expected}"
}

# The default remains DWARF-based so existing profiles retain their semantics.
default_perf_command="$("${RUNNER}" --print-perf-command video_room_small)"
assert_contains "${default_perf_command}" "--call-graph dwarf,16384"

# LBR is opt-in and its printed command is inspectable without perf privileges.
lbr_perf_command="$("${RUNNER}" --attribution-mode lbr --print-perf-command video_room_small)"
assert_contains "${lbr_perf_command}" "--call-graph lbr"

# A separate network namespace must use a reachable, non-loopback client URL.
netns_load_command="$(OXIDESFU_PROFILE_BIND=192.0.2.10:7880 OXIDESFU_PROFILE_URL=http://192.0.2.10:7880 \
    "${RUNNER}" --client-netns profile-clients --print-load-command video_room_small)"
assert_contains "${netns_load_command}" "ip netns exec profile-clients lk"
assert_fails_with "requires OXIDESFU_PROFILE_URL to be non-loopback" \
    "${RUNNER}" --client-netns profile-clients --print-load-command video_room_small

echo "profile-load-test.sh option tests passed"
