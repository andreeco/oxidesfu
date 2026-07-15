#!/usr/bin/env bash
# Profile an OxideSFU lk perf load-test scenario with perf and generate a flamegraph.
set -euo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

usage() {
    cat <<'EOF'
Usage:
  tools/profiling/profile-load-test.sh [OPTIONS] <scenario>

Run one OxideSFU media-load scenario under perf and write perf.data, a flamegraph,
server logs, and run metadata under target/profiles/.

Scenarios:
  audio_fanout_small
  video_room_small
  audio_fanout_medium
  livestream_medium
  mixed_room_medium
  video_room_high_simulcast_large
  mixed_room_high_simulcast_large

Options:
  --duration <duration>          Override the scenario duration (for example: 90s).
  --output-dir <path>            Store artifacts in this directory instead of target/profiles/.
  --attribution-mode <mode>      Call-chain mode: dwarf (default) or lbr (opt-in).
  --client-netns <namespace>     Run lk in an already configured network namespace.
  --print-load-command           Print the resolved lk command without building or starting anything.
  --print-perf-command           Print the resolved perf command without building or starting anything.
  --list                         List scenarios.
  -h, --help             Show this help.

LBR attribution:
  --attribution-mode lbr captures branch-stack call chains, which can identify
  user-space callers below __vdso_clock_gettime. It is supported only when the
  host perf/kernel/CPU permit LBR sampling; the runner preflights it before the
  server starts. Default DWARF capture is unchanged.

Network namespace clients:
  --client-netns only wraps lk with `ip netns exec`; it does not configure a
  namespace, routes, NAT, or firewall rules. Set OXIDESFU_PROFILE_URL to a
  non-loopback server address reachable from that namespace (and normally set
  OXIDESFU_PROFILE_BIND to that host address) to avoid loopback kernel/epoll
  work being attributed to the server process.

Examples:
  tools/profiling/profile-load-test.sh video_room_high_simulcast_large
  tools/profiling/profile-load-test.sh --duration 90s mixed_room_high_simulcast_large
  tools/profiling/profile-load-test.sh --attribution-mode lbr mixed_room_high_simulcast_large
  OXIDESFU_PROFILE_BIND=192.0.2.10:7880 OXIDESFU_PROFILE_URL=http://192.0.2.10:7880 \
    tools/profiling/profile-load-test.sh --client-netns profile-clients video_room_small
EOF
}

list_scenarios() {
    cat <<'EOF'
audio_fanout_small
video_room_small
audio_fanout_medium
livestream_medium
mixed_room_medium
video_room_high_simulcast_large
mixed_room_high_simulcast_large
EOF
}

scenario=""
duration_override=""
output_root="${WORKSPACE_ROOT}/target/profiles"
attribution_mode="dwarf"
client_netns=""
print_load_command=false
print_perf_command=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)
            [[ $# -ge 2 ]] || { echo "--duration requires a value" >&2; exit 2; }
            duration_override="$2"
            shift 2
            ;;
        --output-dir)
            [[ $# -ge 2 ]] || { echo "--output-dir requires a value" >&2; exit 2; }
            output_root="$2"
            shift 2
            ;;
        --attribution-mode)
            [[ $# -ge 2 ]] || { echo "--attribution-mode requires a value" >&2; exit 2; }
            attribution_mode="$2"
            shift 2
            ;;
        --client-netns)
            [[ $# -ge 2 ]] || { echo "--client-netns requires a value" >&2; exit 2; }
            client_netns="$2"
            shift 2
            ;;
        --print-load-command)
            print_load_command=true
            shift
            ;;
        --print-perf-command)
            print_perf_command=true
            shift
            ;;
        --list)
            list_scenarios
            exit 0
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -*)
            echo "unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
        *)
            [[ -z "${scenario}" ]] || { echo "only one scenario may be selected" >&2; exit 2; }
            scenario="$1"
            shift
            ;;
    esac
done

[[ -n "${scenario}" ]] || { usage >&2; exit 2; }

duration=""
video_publishers=""
audio_publishers=""
subscribers=""
num_per_second=""
layout=""
video_resolution=""
no_simulcast=false

case "${scenario}" in
    audio_fanout_small)
        duration="6s"; video_publishers=0; audio_publishers=2; subscribers=4
        num_per_second=10; layout=speaker; video_resolution=low; no_simulcast=true
        ;;
    video_room_small)
        duration="6s"; video_publishers=1; audio_publishers=0; subscribers=2
        num_per_second=10; layout=3x3; video_resolution=low; no_simulcast=true
        ;;
    audio_fanout_medium)
        duration="10s"; video_publishers=0; audio_publishers=4; subscribers=12
        num_per_second=12; layout=speaker; video_resolution=low; no_simulcast=true
        ;;
    livestream_medium)
        duration="10s"; video_publishers=1; audio_publishers=0; subscribers=20
        num_per_second=15; layout=speaker; video_resolution=low; no_simulcast=true
        ;;
    mixed_room_medium)
        duration="10s"; video_publishers=2; audio_publishers=2; subscribers=10
        num_per_second=12; layout=3x3; video_resolution=low; no_simulcast=true
        ;;
    video_room_high_simulcast_large)
        duration="30s"; video_publishers=3; audio_publishers=0; subscribers=18
        num_per_second=20; layout=3x3; video_resolution=high
        ;;
    mixed_room_high_simulcast_large)
        duration="30s"; video_publishers=4; audio_publishers=4; subscribers=20
        num_per_second=20; layout=speaker; video_resolution=high
        ;;
    *)
        echo "unknown scenario: ${scenario}" >&2
        echo "valid scenarios:" >&2
        list_scenarios >&2
        exit 2
        ;;
esac

if [[ -n "${duration_override}" ]]; then
    duration="${duration_override}"
fi

case "${attribution_mode}" in
    dwarf)
        perf_call_graph="dwarf,16384"
        ;;
    lbr)
        perf_call_graph="lbr"
        ;;
    *)
        echo "unknown attribution mode: ${attribution_mode} (expected dwarf or lbr)" >&2
        exit 2
        ;;
esac

readonly API_KEY="${OXIDESFU_PROFILE_API_KEY:-devkey}"
readonly API_SECRET="${OXIDESFU_PROFILE_API_SECRET:-secret}"
readonly BIND_ADDR="${OXIDESFU_PROFILE_BIND:-127.0.0.1:7880}"
readonly BASE_URL="${OXIDESFU_PROFILE_URL:-http://${BIND_ADDR}}"
readonly ROOM_NAME="profile-${scenario}-$(date -u +%Y%m%dT%H%M%SZ)"

load_command=(
    lk --url "${BASE_URL}" --api-key "${API_KEY}" --api-secret "${API_SECRET}" --yes
    perf load-test
    --room "${ROOM_NAME}"
    --duration "${duration}"
    --video-publishers "${video_publishers}"
    --audio-publishers "${audio_publishers}"
    --subscribers "${subscribers}"
    --num-per-second "${num_per_second}"
    --layout "${layout}"
    --video-resolution "${video_resolution}"
)
if [[ "${no_simulcast}" == true ]]; then
    load_command+=(--no-simulcast)
fi

client_command=("${load_command[@]}")
if [[ -n "${client_netns}" ]]; then
    case "${BASE_URL}" in
        http://127.*|https://127.*|http://localhost*|https://localhost*|http://[[]::1]*|https://[[]::1]*)
            echo "--client-netns requires OXIDESFU_PROFILE_URL to be non-loopback and reachable from the namespace" >&2
            exit 2
            ;;
    esac
    client_command=(ip netns exec "${client_netns}" "${load_command[@]}")
fi

if [[ "${print_load_command}" == true ]]; then
    printf '%q ' "${client_command[@]}"
    printf '\n'
    exit 0
fi

if [[ "${print_perf_command}" == true ]]; then
    printf 'perf record --pid <server-pid> --call-graph %s --freq 999 --output <artifact-dir>/perf.data\n' "${perf_call_graph}"
    exit 0
fi

require_command() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "required command not found: $1" >&2
        exit 1
    }
}

for command in cargo curl git inferno-collapse-perf inferno-flamegraph lk perf; do
    require_command "${command}"
done
if [[ -n "${client_netns}" ]]; then
    require_command ip
    ip netns exec "${client_netns}" true || {
        echo "unable to enter network namespace: ${client_netns}" >&2
        exit 1
    }
fi

run_id="${scenario}-$(date -u +%Y%m%dT%H%M%SZ)-$(git -C "${WORKSPACE_ROOT}" rev-parse --short HEAD)"
artifact_dir="${output_root}/${run_id}"
mkdir -p "${artifact_dir}"

server_pid=""
perf_pid=""
cleanup() {
    local status=$?
    if [[ -n "${perf_pid}" ]] && kill -0 "${perf_pid}" 2>/dev/null; then
        kill -INT "${perf_pid}" 2>/dev/null || true
        wait "${perf_pid}" || true
    fi
    if [[ -n "${server_pid}" ]] && kill -0 "${server_pid}" 2>/dev/null; then
        kill -TERM "${server_pid}" 2>/dev/null || true
        wait "${server_pid}" || true
    fi
    exit "${status}"
}
trap cleanup EXIT INT TERM

if [[ "${attribution_mode}" == "lbr" ]]; then
    printf 'Checking LBR call-chain support...\n'
    if ! perf record --call-graph lbr --freq 999 \
        --output "${artifact_dir}/lbr-capability-check.data" -- true \
        >"${artifact_dir}/lbr-capability-check.log" 2>&1; then
        echo "LBR call-chain capture is unavailable; see ${artifact_dir}/lbr-capability-check.log" >&2
        echo "Use the default --attribution-mode dwarf on this host." >&2
        exit 1
    fi
    rm -f "${artifact_dir}/lbr-capability-check.data"
fi

cat >"${artifact_dir}/metadata.txt" <<EOF
scenario=${scenario}
room=${ROOM_NAME}
base_url=${BASE_URL}
duration=${duration}
attribution_mode=${attribution_mode}
perf_call_graph=${perf_call_graph}
client_netns=${client_netns:-none}
git_revision=$(git -C "${WORKSPACE_ROOT}" rev-parse HEAD)
rustc=$(rustc --version)
kernel=$(uname -srmo)
command=$(printf '%q ' "${client_command[@]}")
EOF

printf 'Building profileable OxideSFU server...\n'
cargo build -p oxidesfu-server --profile profiling --manifest-path "${WORKSPACE_ROOT}/Cargo.toml"

printf 'Starting OxideSFU on %s...\n' "${BIND_ADDR}"
RUST_LOG=error "${WORKSPACE_ROOT}/target/profiling/oxidesfu-server" \
    --bind "${BIND_ADDR}" \
    --api-key "${API_KEY}" \
    --api-secret "${API_SECRET}" \
    >"${artifact_dir}/server.log" 2>&1 &
server_pid=$!

for _ in $(seq 1 100); do
    if ! kill -0 "${server_pid}" 2>/dev/null; then
        echo "OxideSFU exited before becoming ready; see ${artifact_dir}/server.log" >&2
        exit 1
    fi
    if curl --fail --silent --output /dev/null "${BASE_URL}/healthz"; then
        break
    fi
    sleep 0.2
done
curl --fail --silent --output /dev/null "${BASE_URL}/healthz" || {
    echo "OxideSFU did not become ready; see ${artifact_dir}/server.log" >&2
    exit 1
}

printf 'Recording perf data for %s...\n' "${scenario}"
perf record \
    --pid "${server_pid}" \
    --call-graph "${perf_call_graph}" \
    --freq 999 \
    --output "${artifact_dir}/perf.data" \
    >"${artifact_dir}/perf.log" 2>&1 &
perf_pid=$!
sleep 1

printf 'Running: '
printf '%q ' "${client_command[@]}"
printf '\n'
"${client_command[@]}" | tee "${artifact_dir}/load-test.log"

# perf writes its data file when interrupted and exits with SIGINT status.
# That is expected after a completed load test, not a profiling failure.
kill -INT "${perf_pid}"
wait "${perf_pid}" || true
perf_pid=""

perf script --input "${artifact_dir}/perf.data" \
    | inferno-collapse-perf \
    | inferno-flamegraph \
    >"${artifact_dir}/flamegraph.svg"

printf 'Profile complete.\n'
printf '  Perf data:  %s\n' "${artifact_dir}/perf.data"
printf '  Flamegraph: %s\n' "${artifact_dir}/flamegraph.svg"
printf '  Metadata:   %s\n' "${artifact_dir}/metadata.txt"
