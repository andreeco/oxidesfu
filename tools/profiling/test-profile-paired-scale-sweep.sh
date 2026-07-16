#!/usr/bin/env bash
# Script-level contract tests for profile-paired-scale-sweep.sh planning behavior.
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly RUNNER="${SCRIPT_DIR}/profile-paired-scale-sweep.sh"

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

plan="$("${RUNNER}" --print-plan mixed_room_high_simulcast_large)"
assert_contains "${plan}" "scenario=mixed_room_high_simulcast_large"
assert_contains "${plan}" "point=baseline video_publishers=4 audio_publishers=4 subscribers=20"
assert_contains "${plan}" "point=video_only video_publishers=4 audio_publishers=0 subscribers=20"
assert_contains "${plan}" "point=audio_only video_publishers=0 audio_publishers=4 subscribers=20"
assert_contains "${plan}" "point=subscribers_10 video_publishers=4 audio_publishers=4 subscribers=10"
assert_contains "${plan}" "point=subscribers_15 video_publishers=4 audio_publishers=4 subscribers=15"
assert_contains "${plan}" "run=1 order=go_livekit,oxidesfu"
assert_contains "${plan}" "client_media_evidence=post-warmup Rust SDK inbound RTP stats"
assert_contains "${plan}" "client_media_evidence_warmup_ms=5000"
assert_contains "${plan}" "client_media_evidence_window_ms=5000"

second_run_plan="$("${RUNNER}" --runs 2 --print-plan mixed_room_high_simulcast_large)"
assert_contains "${second_run_plan}" "run=2 order=oxidesfu,go_livekit"

netns_plan="$(OXIDESFU_PROFILE_BIND=192.0.2.10 OXIDESFU_PROFILE_URL=http://192.0.2.10 \
    "${RUNNER}" --client-netns profile-clients --print-plan mixed_room_high_simulcast_large)"
assert_contains "${netns_plan}" "client_netns=profile-clients"
assert_contains "${netns_plan}" "ip netns exec profile-clients lk"

assert_fails_with "requires OXIDESFU_PROFILE_URL to be non-loopback" \
    "${RUNNER}" --client-netns profile-clients --print-plan mixed_room_high_simulcast_large
assert_fails_with "only supports mixed_room_high_simulcast_large" \
    "${RUNNER}" --print-plan audio_fanout_medium

echo "profile-paired-scale-sweep.sh plan tests passed"
