# Profiling OxideSFU

This guide profiles the optimized server under a real `lk perf load-test` workload.
The benchmark suite answers whether a change improved process-level CPU/RSS; the
profilers below identify the functions, tasks, locks, or allocations responsible.

## Reference map

- `crates/oxidesfu-test/src/benchmark/load.rs` defines the existing client-driven
  scenarios and launches `lk perf load-test` against a release server.
- `crates/oxidesfu-test/src/benchmark/README.md` documents the comparison
  artifacts and scenario controls.
- `console-subscriber` 0.5.0 documentation/API was inspected through
  `cargo info console-subscriber@0.5.0` (Tokio Console subscriber layer;
  Rust 1.74+).
- Tokio 1.52.3 feature metadata was inspected through `cargo info tokio@1.52.3`;
  the `tracing` feature supplies the instrumentation consumed by Tokio Console.
- The workspace currently pins `webrtc-rs` to
  `24b69d02220ffdaf67af4550482d5986089a95aa` (RTC
  `6d436970b437bb8c7572e4ab8d970333496a1edb`).

## Linux packages and tools

Install the native tools appropriate to the distribution:

```sh
# Debian.
sudo apt install linux-perf bpftrace heaptrack

# Ubuntu host: use the perf package matching the running kernel.
sudo apt install linux-tools-common linux-tools-$(uname -r) bpftrace heaptrack

# The project devcontainer already includes linux-tools-generic, bpftrace, and heaptrack.
# Rebuild it after pulling these changes. Its default security posture intentionally
# does not grant profiler capabilities. For local container profiling only, add
# PERFMON, BPF, and SYS_PTRACE capabilities plus seccomp=unconfined in an uncommitted
# local devcontainer override. The host still controls perf/eBPF availability and
# perf_event_paranoid.

# Fedora/RHEL-family equivalent.
sudo dnf install perf bpftrace heaptrack

# Rust CLI tools.
cargo install inferno --locked
cargo install tokio-console --locked
```

`perf` needs permission to sample. On a dedicated development host, enable
unprivileged user-space profiling for the current boot:

```sh
sudo sysctl -w kernel.perf_event_paranoid=1
```

Do not relax this setting on a shared or production host. `perf` must be
installed and `perf_event_paranoid` must not be `3`; otherwise sampling fails.

## Build a profileable server

The workspace `profiling` Cargo profile keeps release optimization while retaining
debug symbols. It leaves normal `release` artifacts unchanged:

```sh
cargo build -p oxidesfu-server --profile profiling
```

Run the server from `target/profiling/`:

```sh
RUST_LOG=error ./target/profiling/oxidesfu-server \
  --bind 127.0.0.1:7880 \
  --api-key devkey \
  --api-secret secret
```

## CPU flamegraph from a benchmark scenario

Use the repository-owned runner to profile any real media scenario defined in
`crates/oxidesfu-test/src/benchmark/load.rs`. It builds the profileable server,
owns its lifecycle, records `perf`, runs the matching `lk perf load-test`
arguments, and writes `perf.data`, `flamegraph.svg`, logs, and metadata under
`target/profiles/`.

```sh
tools/profiling/profile-load-test.sh --list
tools/profiling/profile-load-test.sh video_room_high_simulcast_large
tools/profiling/profile-load-test.sh --duration 90s mixed_room_high_simulcast_large
```

Use `--print-load-command` to inspect a preset without starting a server. The
runner deliberately covers only the seven media-load scenarios; the
`unit_summary_output` benchmark artifact is a unit test and has no `lk` workload.

## Paired Go/Oxide high-mixed scale profiles

`profile-paired-scale-sweep.sh` profiles the real Go LiveKit server and OxideSFU
separately with identical `lk` traffic. It exists specifically for the
high-mixed benchmark regression: it varies one workload dimension at a time
while preserving independent `perf.data`, flamegraph, server-log, load-log, and
metadata artifacts for each implementation.

```sh
# Inspect the five fixed scale points and alternating order without starting processes.
tools/profiling/profile-paired-scale-sweep.sh --print-plan \
  mixed_room_high_simulcast_large

# Run one 60-second Go/Oxide pair per point.
tools/profiling/profile-paired-scale-sweep.sh --duration 60s --runs 1 \
  mixed_room_high_simulcast_large
```

The default Go checkout is `../othercode/livekit`; pass `--livekit-root` or set
`OXIDESFU_LIVEKIT_ROOT` if it lives elsewhere. The runner builds the Go
`cmd/server` binary and OxideSFU's `profiling` binary once, then runs Go before
Oxide on odd repetitions and Oxide before Go on even ones to reduce thermal and
background-order bias. Its plan is: baseline (`4V/4A/20S`), video-only
(`4V/0A/20S`), audio-only (`0V/4A/20S`), and subscriber reductions
(`4V/4A/10S`, `4V/4A/15S`).

It is an opt-in investigation tool, not a benchmark gate. Use DWARF on the
current AMD host: raw branch-stack recording works, but `perf --call-graph lbr`
is unsupported. A live 5-second lifecycle sweep has been validated; retain the
full profile artifacts under `target/profiles/` and require complete `lk`
delivery before drawing a performance conclusion.

The commands below remain useful for ad hoc profiling with a custom server or
load shape. In a second terminal, attach `perf` to the server after its Tokio
worker threads have started:

```sh
perf record \
  --pid "$(pgrep -n oxidesfu-server)" \
  --call-graph dwarf,16384 \
  --freq 999 \
  --output target/oxide-cpu.data
```

In a third terminal, generate a representative simulcast workload:

```sh
lk --url http://127.0.0.1:7880 --api-key devkey --api-secret secret --yes \
  perf load-test \
  --room profile-video \
  --duration 90s \
  --video-publishers 3 \
  --audio-publishers 3 \
  --subscribers 18 \
  --num-per-second 20 \
  --layout 3x3 \
  --video-resolution high
```

Stop `perf record` after the run, then inspect either the interactive report or
an SVG flamegraph:

```sh
perf report --input target/oxide-cpu.data
perf script --input target/oxide-cpu.data \
  | inferno-collapse-perf \
  | inferno-flamegraph > target/oxide-cpu.svg
```

Do not begin optimization until this identifies a dominant stack. If allocator,
serialization, RTP routing, crypto, or locking dominates, preserve this profile
as the before artifact and repeat the exact workload after each focused change.

## Tokio task profiling

The `tokio-console` feature is opt-in and starts a task inspector on loopback
only. It requires Tokio's unstable instrumentation cfg at compile time:

```sh
RUSTFLAGS="--cfg tokio_unstable" \
  cargo run -p oxidesfu-server --profile profiling --features tokio-console -- \
  --bind 127.0.0.1:7880 --api-key devkey --api-secret secret
```

Connect locally in another terminal:

```sh
tokio-console
```

The default inspector endpoint is `127.0.0.1:6669`. To use a different local
endpoint, set `OXIDESFU_TOKIO_CONSOLE_ADDR`, for example
`127.0.0.1:7777`. Do not bind it to an untrusted network; Console exposes
runtime internals and has no authentication.

Use Tokio Console when a CPU profile is inconclusive, latency rises while CPU is
not saturated, or you suspect task wake-up/poll delay.

## Lock and allocation follow-up

For suspected lock contention, collect waiting-lock data during the identical
load test:

```sh
perf lock record --pid "$(pgrep -n oxidesfu-server)"
perf lock report
```

For suspected allocation/copy pressure, run the profileable server under
Heaptrack, execute the same load, then inspect the generated profile:

```sh
heaptrack ./target/profiling/oxidesfu-server \
  --bind 127.0.0.1:7880 --api-key devkey --api-secret secret
```

Heaptrack and Tokio Console affect execution. Use them to locate causes, then
validate improvements with a clean `perf` recording and the existing benchmark
suite:

```sh
OXIDESFU_ENABLE_BENCHMARKS=1 OXIDESFU_BENCHMARK_MODE=full \
  OXIDESFU_BENCHMARK_RUNS=5 \
  cargo test -p oxidesfu-test benchmark_ -- --nocapture
```
