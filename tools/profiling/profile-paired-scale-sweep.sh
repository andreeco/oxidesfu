#!/usr/bin/env bash
# Profile Go LiveKit and OxideSFU under matched high-mixed scale points.
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
readonly DEFAULT_LIVEKIT_ROOT="${WORKSPACE_ROOT}/../othercode/livekit"
readonly API_KEY="${OXIDESFU_PROFILE_API_KEY:-devkey}"
readonly API_SECRET="${OXIDESFU_PROFILE_API_SECRET:-secret}"

usage() {
    cat <<'EOF'
Usage:
  tools/profiling/profile-paired-scale-sweep.sh [OPTIONS] mixed_room_high_simulcast_large

Profile Go LiveKit and OxideSFU separately under the same high mixed simulcast
load points. Each point runs one implementation at a time, preserving separate
perf.data/flamegraph artifacts and alternating Go/Oxide run order.

Options:
  --duration <duration>       Load-test duration (default: 30s).
  --runs <count>              Repetitions per point and implementation (default: 1).
  --output-dir <path>         Artifact parent (default: target/profiles/).
  --livekit-root <path>       Go LiveKit checkout (default: ../othercode/livekit).
  --attribution-mode <mode>   dwarf (default) or lbr.
  --client-netns <namespace>  Run lk from a configured non-loopback namespace.
  --print-plan                Print the resolved points and commands; do not build/run.
  -h, --help                  Show this help.

The fixed one-factor plan is:
  baseline       4 video, 4 audio, 20 subscribers
  video_only     4 video, 0 audio, 20 subscribers
  audio_only     0 video, 4 audio, 20 subscribers
  subscribers_10 4 video, 4 audio, 10 subscribers
  subscribers_15 4 video, 4 audio, 15 subscribers

Use a routed namespace or a remote client for transport attribution. This script
only enters an existing namespace; it does not create routes, NAT, or firewall
rules.
EOF
}

scenario=""
duration="30s"
runs=1
output_root="${WORKSPACE_ROOT}/target/profiles"
livekit_root="${OXIDESFU_LIVEKIT_ROOT:-${DEFAULT_LIVEKIT_ROOT}}"
attribution_mode="dwarf"
client_netns=""
print_plan=false
media_evidence_warmup_ms="${OXIDESFU_PROFILE_MEDIA_EVIDENCE_WARMUP_MS:-5000}"
media_evidence_window_ms="${OXIDESFU_PROFILE_MEDIA_EVIDENCE_WINDOW_MS:-5000}"
media_evidence_sample_interval_ms="${OXIDESFU_PROFILE_MEDIA_EVIDENCE_SAMPLE_INTERVAL_MS:-1000}"
bind_host="${OXIDESFU_PROFILE_BIND:-127.0.0.1}"
base_url_origin="${OXIDESFU_PROFILE_URL:-http://${bind_host}}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)
            [[ $# -ge 2 ]] || { echo "--duration requires a value" >&2; exit 2; }
            duration="$2"; shift 2 ;;
        --runs)
            [[ $# -ge 2 ]] || { echo "--runs requires a value" >&2; exit 2; }
            runs="$2"; shift 2 ;;
        --output-dir)
            [[ $# -ge 2 ]] || { echo "--output-dir requires a value" >&2; exit 2; }
            output_root="$2"; shift 2 ;;
        --livekit-root)
            [[ $# -ge 2 ]] || { echo "--livekit-root requires a value" >&2; exit 2; }
            livekit_root="$2"; shift 2 ;;
        --attribution-mode)
            [[ $# -ge 2 ]] || { echo "--attribution-mode requires a value" >&2; exit 2; }
            attribution_mode="$2"; shift 2 ;;
        --client-netns)
            [[ $# -ge 2 ]] || { echo "--client-netns requires a value" >&2; exit 2; }
            client_netns="$2"; shift 2 ;;
        --print-plan)
            print_plan=true; shift ;;
        -h|--help)
            usage; exit 0 ;;
        -*)
            echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
        *)
            [[ -z "${scenario}" ]] || { echo "only one scenario may be selected" >&2; exit 2; }
            scenario="$1"; shift ;;
    esac
done

[[ "${scenario}" == "mixed_room_high_simulcast_large" ]] || {
    echo "profile-paired-scale-sweep.sh only supports mixed_room_high_simulcast_large" >&2
    exit 2
}
[[ "${runs}" =~ ^[1-9][0-9]*$ ]] || { echo "--runs must be a positive integer" >&2; exit 2; }

case "${attribution_mode}" in
    dwarf) perf_call_graph="dwarf,16384" ;;
    lbr) perf_call_graph="lbr" ;;
    *) echo "unknown attribution mode: ${attribution_mode} (expected dwarf or lbr)" >&2; exit 2 ;;
esac

if [[ -n "${client_netns}" ]]; then
    case "${base_url_origin}" in
        http://127.*|https://127.*|http://localhost*|https://localhost*|http://[[]::1]*|https://[[]::1]*)
            echo "--client-netns requires OXIDESFU_PROFILE_URL to be non-loopback and reachable from the namespace" >&2
            exit 2 ;;
    esac
fi

point_names=(baseline video_only audio_only subscribers_10 subscribers_15)
point_video_publishers=(4 4 0 4 4)
point_audio_publishers=(4 0 4 4 4)
point_subscribers=(20 20 20 10 15)

load_command_for_point() {
    local base_url="$1"
    local point_index="$2"
    local room="${3:-paired-${scenario}-${point_names[point_index]}-$(date -u +%Y%m%dT%H%M%SZ)-${RANDOM}}"
    local -a command=(
        lk --url "${base_url}" --api-key "${API_KEY}" --api-secret "${API_SECRET}" --yes
        perf load-test --room "${room}" --duration "${duration}"
        --video-publishers "${point_video_publishers[point_index]}"
        --audio-publishers "${point_audio_publishers[point_index]}"
        --subscribers "${point_subscribers[point_index]}"
        --num-per-second 20 --layout speaker --video-resolution high
    )
    if [[ -n "${client_netns}" ]]; then
        command=(ip netns exec "${client_netns}" "${command[@]}")
    fi
    printf '%q ' "${command[@]}"
    printf '\n'
}

print_resolved_plan() {
    echo "scenario=${scenario}"
    echo "duration=${duration}"
    echo "runs=${runs}"
    echo "attribution_mode=${attribution_mode}"
    echo "client_netns=${client_netns:-none}"
    echo "livekit_root=${livekit_root}"
    echo "client_media_evidence=post-warmup Rust SDK inbound RTP stats"
    echo "client_media_evidence_warmup_ms=${media_evidence_warmup_ms}"
    echo "client_media_evidence_window_ms=${media_evidence_window_ms}"
    echo "client_media_evidence_sample_interval_ms=${media_evidence_sample_interval_ms}"
    for index in "${!point_names[@]}"; do
        echo "point=${point_names[index]} video_publishers=${point_video_publishers[index]} audio_publishers=${point_audio_publishers[index]} subscribers=${point_subscribers[index]}"
    done
    for run in $(seq 1 "${runs}"); do
        if (( run % 2 == 1 )); then
            echo "run=${run} order=go_livekit,oxidesfu"
        else
            echo "run=${run} order=oxidesfu,go_livekit"
        fi
    done
    echo "example_load_command=$(load_command_for_point \"${base_url_origin}:http-port\" 0 paired-example-room)"
    echo "perf_command=perf record --pid <server-pid> --call-graph ${perf_call_graph} --freq 999 --output <artifact-dir>/perf.data"
}

if [[ "${print_plan}" == true ]]; then
    print_resolved_plan
    exit 0
fi

require_command() {
    command -v "$1" >/dev/null 2>&1 || { echo "required command not found: $1" >&2; exit 1; }
}
for command in cargo curl go inferno-collapse-perf inferno-flamegraph lk perf python3; do
    require_command "${command}"
done
if [[ -n "${client_netns}" ]]; then
    require_command ip
    ip netns exec "${client_netns}" true || { echo "unable to enter network namespace: ${client_netns}" >&2; exit 1; }
fi
[[ -d "${livekit_root}" ]] || { echo "Go LiveKit checkout not found: ${livekit_root}" >&2; exit 1; }
[[ -f "${livekit_root}/cmd/server/main.go" ]] || { echo "Go LiveKit server source not found under: ${livekit_root}" >&2; exit 1; }

run_id="paired-${scenario}-$(date -u +%Y%m%dT%H%M%SZ)-$(git -C "${WORKSPACE_ROOT}" rev-parse --short HEAD)"
artifact_dir="${output_root}/${run_id}"
mkdir -p "${artifact_dir}"

go_binary="${artifact_dir}/livekit-server"
oxide_binary="${WORKSPACE_ROOT}/target/profiling/oxidesfu-server"
active_server_pid=""
active_perf_pid=""

cleanup() {
    local status=$?
    if [[ -n "${active_perf_pid}" ]] && kill -0 "${active_perf_pid}" 2>/dev/null; then
        kill -INT "${active_perf_pid}" 2>/dev/null || true
        wait "${active_perf_pid}" 2>/dev/null || true
    fi
    if [[ -n "${active_server_pid}" ]] && kill -0 "${active_server_pid}" 2>/dev/null; then
        kill -TERM "${active_server_pid}" 2>/dev/null || true
        wait "${active_server_pid}" 2>/dev/null || true
    fi
    exit "${status}"
}
trap cleanup EXIT INT TERM

reserve_port() {
    python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

wait_ready() {
    local base_url="$1"
    local server_pid="$2"
    for _ in $(seq 1 100); do
        if ! kill -0 "${server_pid}" 2>/dev/null; then
            return 1
        fi
        if lk --url "${base_url}" --api-key "${API_KEY}" --api-secret "${API_SECRET}" --yes room list >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

write_metadata() {
    local run_dir="$1"
    local implementation="$2"
    local point_index="$3"
    local base_url="$4"
    cat >"${run_dir}/metadata.txt" <<EOF
scenario=${scenario}
implementation=${implementation}
point=${point_names[point_index]}
video_publishers=${point_video_publishers[point_index]}
audio_publishers=${point_audio_publishers[point_index]}
subscribers=${point_subscribers[point_index]}
duration=${duration}
base_url=${base_url}
attribution_mode=${attribution_mode}
perf_call_graph=${perf_call_graph}
client_netns=${client_netns:-none}
client_media_evidence=client-media-evidence.json
client_media_evidence_warmup_ms=${media_evidence_warmup_ms}
client_media_evidence_window_ms=${media_evidence_window_ms}
client_media_evidence_sample_interval_ms=${media_evidence_sample_interval_ms}
oxidesfu_revision=$(git -C "${WORKSPACE_ROOT}" rev-parse HEAD)
livekit_revision=$(git -C "${livekit_root}" rev-parse HEAD)
kernel=$(uname -srmo)
perf=$(perf --version)
EOF
}

run_profile() {
    local implementation="$1"
    local point_index="$2"
    local run_number="$3"
    local point_dir="${artifact_dir}/${point_names[point_index]}/run-$(printf '%02d' "${run_number}")/${implementation}"
    mkdir -p "${point_dir}"

    local http_port rtc_tcp_port rtc_udp_port base_url server_pid perf_pid
    http_port="$(reserve_port)"
    rtc_tcp_port="$(reserve_port)"
    rtc_udp_port="$(reserve_port)"
    base_url="${base_url_origin}:${http_port}"

    if [[ "${implementation}" == "go_livekit" ]]; then
        "${go_binary}" --dev --bind "${bind_host}" --port "${http_port}" \
            --rtc.tcp_port "${rtc_tcp_port}" --udp-port "${rtc_udp_port}" \
            --keys "${API_KEY}: ${API_SECRET}" >"${point_dir}/server.log" 2>&1 &
    else
        RUST_LOG=error "${oxide_binary}" --bind "${bind_host}:${http_port}" \
            --api-key "${API_KEY}" --api-secret "${API_SECRET}" >"${point_dir}/server.log" 2>&1 &
    fi
    server_pid=$!
    active_server_pid="${server_pid}"

    if ! wait_ready "${base_url}" "${server_pid}"; then
        kill -TERM "${server_pid}" 2>/dev/null || true
        wait "${server_pid}" 2>/dev/null || true
        active_server_pid=""
        echo "${implementation} did not become ready; see ${point_dir}/server.log" >&2
        return 1
    fi

    write_metadata "${point_dir}" "${implementation}" "${point_index}" "${base_url}"
    perf record --pid "${server_pid}" --call-graph "${perf_call_graph}" --freq 999 \
        --output "${point_dir}/perf.data" >"${point_dir}/perf.log" 2>&1 &
    perf_pid=$!
    active_perf_pid="${perf_pid}"
    sleep 1

    local load_command room load_pid
    room="paired-${scenario}-${point_names[point_index]}-$(date -u +%Y%m%dT%H%M%SZ)-${RANDOM}"
    load_command="$(load_command_for_point "${base_url}" "${point_index}" "${room}")"
    printf '%s\n' "${load_command}" >"${point_dir}/load-command.txt"
    eval "${load_command}" >"${point_dir}/load-test.log" 2>&1 &
    load_pid=$!
    if ! OXIDESFU_MEDIA_EVIDENCE_BASE_URL="${base_url}" \
        OXIDESFU_MEDIA_EVIDENCE_ROOM="${room}" \
        OXIDESFU_MEDIA_EVIDENCE_OUTPUT="${point_dir}/client-media-evidence.json" \
        OXIDESFU_MEDIA_EVIDENCE_WARMUP_MS="${media_evidence_warmup_ms}" \
        OXIDESFU_MEDIA_EVIDENCE_WINDOW_MS="${media_evidence_window_ms}" \
        OXIDESFU_MEDIA_EVIDENCE_SAMPLE_INTERVAL_MS="${media_evidence_sample_interval_ms}" \
        cargo test --manifest-path "${WORKSPACE_ROOT}/Cargo.toml" -p oxidesfu-test paired_profile_client_media_evidence_writes_post_warmup_track_stats \
            -- --ignored --nocapture >"${point_dir}/client-media-evidence.log" 2>&1; then
        kill "${load_pid}" 2>/dev/null || true
        wait "${load_pid}" 2>/dev/null || true
        kill -INT "${perf_pid}" 2>/dev/null || true
        wait "${perf_pid}" 2>/dev/null || true
        active_perf_pid=""
        kill -TERM "${server_pid}" 2>/dev/null || true
        wait "${server_pid}" 2>/dev/null || true
        active_server_pid=""
        echo "${implementation} client media evidence failed; see ${point_dir}/client-media-evidence.log" >&2
        return 1
    fi
    if ! wait "${load_pid}"; then
        kill -INT "${perf_pid}" 2>/dev/null || true
        wait "${perf_pid}" 2>/dev/null || true
        active_perf_pid=""
        kill -TERM "${server_pid}" 2>/dev/null || true
        wait "${server_pid}" 2>/dev/null || true
        active_server_pid=""
        echo "${implementation} load test failed; see ${point_dir}/load-test.log" >&2
        return 1
    fi

    if [[ "${implementation}" == "oxidesfu" ]]; then
        curl --fail --silent --show-error "${base_url}/debug/forwarding-snapshots" \
            >"${point_dir}/oxide-forwarding-snapshot.jsonl"
    fi

    kill -INT "${perf_pid}" 2>/dev/null || true
    wait "${perf_pid}" 2>/dev/null || true
    active_perf_pid=""
    inferno-collapse-perf < <(perf script -i "${point_dir}/perf.data") | \
        inferno-flamegraph >"${point_dir}/flamegraph.svg"
    perf report --stdio --no-children -i "${point_dir}/perf.data" \
        --sort overhead,symbol --percent-limit 0.8 >"${point_dir}/perf-report.txt" 2>&1 || true
    kill -TERM "${server_pid}" 2>/dev/null || true
    wait "${server_pid}" 2>/dev/null || true
    active_server_pid=""
}

if [[ "${attribution_mode}" == "lbr" ]]; then
    printf 'Checking LBR call-chain support...\n'
    if ! perf record --call-graph lbr --freq 999 --output "${artifact_dir}/lbr-capability-check.data" -- true \
        >"${artifact_dir}/lbr-capability-check.log" 2>&1; then
        echo "LBR call-chain capture is unavailable; see ${artifact_dir}/lbr-capability-check.log" >&2
        exit 1
    fi
    rm -f "${artifact_dir}/lbr-capability-check.data"
fi

printf 'Building profileable OxideSFU server...\n'
cargo build -p oxidesfu-server --profile profiling --manifest-path "${WORKSPACE_ROOT}/Cargo.toml"
printf 'Prebuilding Rust SDK client media evidence probe...\n'
cargo test --manifest-path "${WORKSPACE_ROOT}/Cargo.toml" -p oxidesfu-test paired_profile_client_media_evidence_writes_post_warmup_track_stats --no-run
printf 'Building Go LiveKit server...\n'
(
    cd "${livekit_root}"
    go build -o "${go_binary}" ./cmd/server
)

print_resolved_plan >"${artifact_dir}/plan.txt"
for point_index in "${!point_names[@]}"; do
    for run_number in $(seq 1 "${runs}"); do
        if (( run_number % 2 == 1 )); then
            run_profile go_livekit "${point_index}" "${run_number}"
            run_profile oxidesfu "${point_index}" "${run_number}"
        else
            run_profile oxidesfu "${point_index}" "${run_number}"
            run_profile go_livekit "${point_index}" "${run_number}"
        fi
    done
done

printf 'Paired scale profiles complete: %s\n' "${artifact_dir}"
