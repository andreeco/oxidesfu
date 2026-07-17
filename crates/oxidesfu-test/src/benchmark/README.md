# OxideSFU benchmark tests

This directory contains the benchmark comparison test suite used by `oxidesfu-test`.

- Test implementation: `crates/oxidesfu-test/src/benchmark/load.rs`
- Execution model: Rust `#[tokio::test]` tests that run `lk perf load-test` against:
  1. upstream Go LiveKit server
  2. local OxideSFU server
- Output artifacts: `target/benchmarks/*.md`, `target/benchmarks/*.json`, and `target/benchmarks/overview.md`

## What these tests measure

For each scenario, the suite compares Go LiveKit vs OxideSFU using:

- wall-clock seconds
- process CPU seconds
- approximate one-core CPU %
- peak and average RSS
- loadavg, network byte/packet deltas, and FD counts (in JSON artifacts)
- subscriber delivery validation: every configured subscriber must receive every expected track and at least one RTP packet; total received RTP packets must remain within a 2× Go/Rust envelope

All video scenarios pin the load generator to H.264 so both implementations forward the same encoded source shape. Benchmark scenarios run serially to avoid inter-scenario contention.

Resource samples are collected from `/proc` during each load test run. CPU and RSS are process-specific. Network counters come from the shared network namespace and are diagnostic only; they are not per-process server accounting.

## Scenarios

Defined in `load.rs`:

Synthetic/fast scenarios:

- `audio_fanout_small`
- `video_room_small`
- `audio_fanout_medium`
- `livestream_medium`
- `mixed_room_medium`

Production-like scenarios (full mode):

- `video_room_high_simulcast_large` (high resolution, simulcast on, longer duration)
- `mixed_room_high_simulcast_large` (high resolution, mixed audio/video, simulcast on, longer duration)

## How to run

From repository root (`oxidesfu/`):

### Smoke mode (quick)

```bash
OXIDESFU_ENABLE_BENCHMARKS=1 cargo test -p oxidesfu-test benchmark_ -- --nocapture
```

### Full mode (all scenarios)

```bash
OXIDESFU_ENABLE_BENCHMARKS=1 OXIDESFU_BENCHMARK_MODE=full cargo test -p oxidesfu-test benchmark_ -- --nocapture
```

### Full mode with multiple runs per scenario (recommended for stability)

```bash
OXIDESFU_ENABLE_BENCHMARKS=1 OXIDESFU_BENCHMARK_MODE=full OXIDESFU_BENCHMARK_RUNS=5 cargo test -p oxidesfu-test benchmark_ -- --nocapture
```

## Environment controls

- `OXIDESFU_ENABLE_BENCHMARKS=1`
  - Required. Without this, benchmark scenario tests print a skip message and return early.
- `OXIDESFU_BENCHMARK_MODE=smoke|full`
  - Default: `smoke`
  - `smoke` skips heavier scenarios (e.g. `*medium*`, `*livestream*`, `*large*`, `*xlarge*`).
  - `full` is required to run production-like `*high_simulcast_large*` scenarios.
- `OXIDESFU_BENCHMARK_RUNS=<N>`
  - Number of repeated runs per implementation and scenario.
  - Default: `1` in smoke mode, `5` in full mode.
- `OXIDESFU_BENCHMARK_MAX_WALL_REGRESSION_PERCENT=<f64>`
  - Regression gate vs Go median wall time. Default `25.0`.
- `OXIDESFU_BENCHMARK_MAX_CPU_REGRESSION_PERCENT=<f64>`
  - Regression gate vs Go median CPU seconds. Default `25.0`.
- `OXIDESFU_BENCHMARK_MAX_PEAK_RSS_REGRESSION_PERCENT=<f64>`
  - Regression gate vs Go median peak RSS. Default `25.0`.
- `OXIDESFU_BENCHMARK_INCLUDE_LOG_TAILS=1`
  - Include sanitized stdout/stderr tails in JSON artifacts.
- `OXIDESFU_BENCHMARK_SERVER_STDIO=1`
  - Show spawned OxideSFU benchmark server stdout/stderr.
- `OXIDESFU_BENCHMARK_PERF_RECORD=<path>`
  - Attach `perf record` to the OxideSFU server PID during the final benchmark run and write a profile at `<path>`.
- `OXIDESFU_BENCHMARK_PERF_RECORD_GO=<path>`
  - Attach `perf record` to the Go LiveKit server PID during the final benchmark run and write a matching profile.

When both are set, the harness profiles Go and OxideSFU sequentially, avoiding concurrent PMU sampling. Profiling requires user-space perf access (for example `kernel.perf_event_paranoid=1`).

## Prerequisites

- `lk` CLI must be available on `PATH` (tests call `lk perf load-test`).
- Go + upstream LiveKit test server support must be available to start the Go reference server.
- Cargo must be available to build `oxidesfu-server --release` for benchmark runs.
- Linux `/proc` access is required for resource sampling.

If prerequisites are missing, benchmark scenario tests may skip with an explanatory message.

## Reading results

- Per-scenario markdown (`*.md`) shows implementation-level median and p95 comparisons.
  - `p95` is computed as nearest-rank over run-level aggregates (not per-request/per-frame latency). With ≤19 runs, p95 equals the max run value.
- Per-scenario JSON (`*.json`) includes scenario config, run summaries, environment metadata, and aggregated stats.
- `overview.md` summarizes the latest artifact per scenario.

## Notes on interpretation

- These are comparative micro/meso benchmarks for CI/dev feedback, not absolute capacity certification.
- Synthetic scenarios are useful for fast regressions; production-like scenarios (`*high_simulcast_large*`) are better for realism.
- Prefer idle hosts and repeated runs (`OXIDESFU_BENCHMARK_RUNS>=5`) before drawing strong conclusions.
- A scenario passes only if both implementations pass subscriber delivery validation and OxideSFU stays within configured regression gates against Go medians.
- CPU/RSS comparisons should only be interpreted from artifacts whose CLI output confirms identical expected track counts and packet-delivery envelope. Packet totals are captured in the CLI log tail when `OXIDESFU_BENCHMARK_INCLUDE_LOG_TAILS=1`.
