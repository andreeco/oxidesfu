# External conformance discovery

These scripts make it easier to run official LiveKit ecosystem test suites against OxideSFU as black-box discovery checks.

The authoritative required test suite remains OxideSFU's internal Rust tests under `crates/oxidesfu-test`. If an external discovery run finds a relevant compatibility bug, add or update an internal OxideSFU test before fixing behavior.

## Important policy

Default discovery uses **clean local checkouts of the official upstream repositories**. You do not need custom `oxidesfu-conformance` branches in the SDK/CLI repositories to run the default full-suite scripts.

For broad upstream `livekit` all-packages reruns that include `github.com/livekit/livekit-server/test` under the OxideSFU harness, use `andreeco/livekit` branch [`oxidesfu/livekit-compat`](https://github.com/andreeco/livekit/tree/oxidesfu/livekit-compat). It includes the test-port configuration needed to run concurrently without `:7880` bind collisions (`LK_TEST_SERVER_PORT` and `LK_TEST_SERVER_PORT_SECOND`).

Older branch-specific focused probes were removed from the primary runner surface. Optional probe snippets that are useful for future work can live under `tools/conformance/reference/`, but they are not required to run upstream suites.

## Linux prerequisites

Install the normal Rust/Go build toolchains plus native packages needed by the upstream SDK/CLI builds.

On Debian/Ubuntu:

```bash
sudo apt update
sudo apt install -y \
  git \
  curl \
  build-essential \
  pkg-config \
  libasound2-dev \
  libopus-dev \
  libopusfile-dev \
  libsoxr-dev \
  clang \
  libclang-dev \
  llvm-dev \
  protobuf-compiler
```

Also required:

- Rust toolchain with `cargo`/`rustc`
- Go toolchain compatible with the local upstream checkouts
- Node.js + npm
- pnpm, or `corepack` available so the runner can use `corepack pnpm`

The `server-sdk-go` runner uses OxideSFU's in-process UDP TURN runtime by default, so coturn is not required. Install coturn only when explicitly selecting the legacy external-TURN comparison backend (`OXIDESFU_DISCOVERY_TURN_BACKEND=coturn`).

## Expected workspace layout

The runners default to this local layout:

```text
oxidesfu/
../othercode/server-sdk-go
../othercode/livekit
../othercode/livekit-cli
../othercode/client-sdk-js
../othercode/rust-sdks
```

Override paths when needed:

```bash
SERVER_SDK_GO=/path/to/server-sdk-go tools/conformance/server-sdk-go-full-suite.sh
LIVEKIT_REPO=/path/to/livekit tools/conformance/livekit-full-suite-all.sh
LIVEKIT_CLI_REPO=/path/to/livekit-cli tools/conformance/livekit-cli-full-suite.sh
CLIENT_SDK_JS_REPO=/path/to/client-sdk-js tools/conformance/client-sdk-js-full-suite.sh
RUST_SDKS_REPO=/path/to/rust-sdks tools/conformance/rust-sdks-full-suite.sh
```

## Shared runner behavior

Unless `OXIDESFU_REUSE_SERVER=true` is set, scripts will:

1. build `oxidesfu-server`,
2. start it at `http://127.0.0.1:7880`,
3. wait until HTTP is reachable,
4. run the upstream suite,
5. stop the spawned server.

When a runner starts its own local server, it intentionally ignores inherited `LIVEKIT_URL` values. This prevents accidentally targeting a real remote LiveKit deployment. `LIVEKIT_URL` is only honored when `OXIDESFU_REUSE_SERVER=true`.

Common environment variables:

- `LIVEKIT_URL` — target OxideSFU HTTP URL when `OXIDESFU_REUSE_SERVER=true`; default `http://127.0.0.1:7880`.
- `LIVEKIT_API_KEY` — API key; default `devkey`.
- `LIVEKIT_API_SECRET` — API secret; default `secret`.
- `OXIDESFU_HOST` — host used when starting OxideSFU; default `127.0.0.1`.
- `OXIDESFU_PORT` — port used when starting OxideSFU; default `7880`.
- `OXIDESFU_REUSE_SERVER` — set to `true` to skip starting a server and use `LIVEKIT_URL`.
- `OXIDESFU_DISCOVERY_LOG_DIR` — local log output directory.
- `OXIDESFU_DISCOVERY_ALLOW_FAILURE` — when `true`, return success even if upstream tests fail.

For Go suites:

- `OXIDESFU_DISCOVERY_GO_TEST_EXTRA_FLAGS` — extra flags appended to `go test`; default `-p 1`.
- `OXIDESFU_DISCOVERY_GO_TEST_SKIP_PATTERN` — optional `go test -skip` regex.
- `OXIDESFU_DISCOVERY_SKIP_REASON` — optional human-readable skip reason.

When a skip pattern is set, runners print a `WARNING` banner and write the skip pattern/reason into the log header. A skip-pattern run is a **partial discovery run**, not full conformance.

Use a custom local port for a spawned server:

```bash
OXIDESFU_PORT=17980 tools/conformance/server-sdk-go-full-suite.sh
```

Reuse an already-running server:

```bash
OXIDESFU_REUSE_SERVER=true \
LIVEKIT_URL=http://127.0.0.1:7880 \
tools/conformance/server-sdk-go-full-suite.sh
```

## Recommended run order

Run from the OxideSFU repo:

```bash
tools/conformance/server-sdk-go-full-suite.sh
OXIDESFU_DISCOVERY_GO_TEST_SKIP_PATTERN='TestSessionE2E$' tools/conformance/livekit-cli-full-suite.sh
tools/conformance/rust-sdks-full-suite.sh
tools/conformance/client-sdk-js-full-suite.sh
OXIDESFU_DISCOVERY_ALLOW_FAILURE=true tools/conformance/livekit-full-suite-all.sh
```

Suggested interpretation:

1. `server-sdk-go-full-suite.sh` — strongest current black-box SDK/server coverage; should run unskipped.
2. `livekit-cli-full-suite.sh` — upstream CLI suite; skip only `TestSessionE2E` unless cloud LLM credentials/runtime are intentionally provisioned.
3. `rust-sdks-full-suite.sh` — official Rust SDK workspace suite.
4. `client-sdk-js-full-suite.sh` — JS unit + smoke/browser packaging suite. If a custom Oxide probe exists in the checkout it runs too; otherwise upstream suite still runs.
5. `livekit-full-suite-all.sh` — official Go LiveKit server repository suite running against the local fork's external-server mode (`LK_EXTERNAL_SERVER_URL`). By default it scopes to the `./test` package and uses parallel Go test settings so Oxide-facing helper tests are fast. Set `OXIDESFU_DISCOVERY_LIVEKIT_PACKAGE_SCOPE=all` for broad upstream discovery across every Go package.

### LiveKit package scope and sharding

The LiveKit runner supports two package scopes:

```bash
# Fast Oxide-facing mode: only the upstream ./test package, with helpers redirected to OxideSFU.
tools/conformance/livekit-full-suite-all.sh

# Broad upstream discovery: all LiveKit Go packages plus the redirected ./test package.
OXIDESFU_DISCOVERY_LIVEKIT_PACKAGE_SCOPE=all \
  OXIDESFU_DISCOVERY_ALLOW_FAILURE=true \
  tools/conformance/livekit-full-suite-all.sh
```

In the default sharded mode, each top-level LiveKit `./test` test gets its own OxideSFU process with isolated HTTP/RTC ports. This preserves the upstream test assumption that `setupSingleNodeTest`/`setupMultiNodeTest` start with fresh server state while still targeting OxideSFU. Each shard also receives a unique local webhook callback port (`shard HTTP port + 50`); the runner configures OxideSFU with that callback before running the corresponding Go test.

The compatibility branch must not use `skipExternalServer` for any top-level `./test` contract mapped by `crates/oxidesfu-test/src/upstream_livekit/`. The runner treats `--- SKIP:` and `no tests to run` output as failures, so an unsupported external configuration is surfaced as a failing Go test rather than a passing partial run.

Sharding controls:

- `OXIDESFU_DISCOVERY_LIVEKIT_SHARD_TESTS=true|false` — enable/disable per-test OxideSFU shards (default `true`).
- `OXIDESFU_DISCOVERY_LIVEKIT_SHARD_WORKERS` — number of tests/OxideSFU instances to run concurrently (default `4`).
- `OXIDESFU_DISCOVERY_LIVEKIT_SHARD_BASE_PORT` — first shard HTTP port (default `18000`).
- `OXIDESFU_DISCOVERY_LIVEKIT_SHARD_PORT_STRIDE` — reserved port stride per shard (default `100`; each shard uses HTTP at base, TCP at `base+1`, UDP range `base+10..base+39`).

Use `OXIDESFU_DISCOVERY_GO_TEST_RUN_PATTERN` to run one LiveKit test while keeping shard startup/targeting behavior identical:

```bash
OXIDESFU_DISCOVERY_GO_TEST_RUN_PATTERN='^TestClientCouldConnect$' \
  OXIDESFU_DISCOVERY_LIVEKIT_SHARD_WORKERS=1 \
  tools/conformance/livekit-full-suite-all.sh
```

## `server-sdk-go` full suite

```bash
tools/conformance/server-sdk-go-full-suite.sh
```

What it does:

- starts or reuses OxideSFU,
- configures OxideSFU's owned UDP TURN runtime by default for TURN discovery tests,
- runs `go test ./... -count=1 -v -p 1` in `server-sdk-go`,
- injects `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`, and `LIVEKIT_KEYS`,
- writes a local run log.

TURN-related overrides:

- `OXIDESFU_DISCOVERY_TURN_MODE` — `auto` (default), `on`, or `off`.
- `OXIDESFU_DISCOVERY_TURN_BACKEND` — `oxide` (default), `auto` (an alias for `oxide`), or `coturn`.
  - `oxide` starts no extra process: it configures the in-process OxideSFU TURN runtime.
  - `coturn` is an explicit legacy external-TURN comparison mode and requires `turnserver`/`coturn` on `PATH`.
- `OXIDESFU_DISCOVERY_TURN_HOST` / `OXIDESFU_DISCOVERY_TURN_PORT` — local TURN bind address; defaults `127.0.0.1:34790`.
- `OXIDESFU_DISCOVERY_TURN_RELAY_MIN_PORT` / `OXIDESFU_DISCOVERY_TURN_RELAY_MAX_PORT` — relay UDP range; defaults `31000-31050`.
- `OXIDESFU_DISCOVERY_TURN_ALLOWED_PEER_CIDRS` — optional test-only restricted-peer entries for script-managed local TURN harnesses.
  - For the owned `oxide` backend, defaults to `127.0.0.0/8` so same-host relay candidates can create permissions.
  - For `coturn`, values are passed as `allowed-peer-ip` and must be plain IP/IP-IP ranges (coturn does not accept CIDR).
- `OXIDESFU_DISCOVERY_TURN_USERNAME` / `OXIDESFU_DISCOVERY_TURN_PASSWORD` — credentials for the explicit coturn backend; default API key/secret. The owned runtime uses participant-specific credentials minted by OxideSFU.

External TURN (when validating a deployed or TCP/TLS-capable TURN service):

- `OXIDESFU_DISCOVERY_EXTERNAL_TURN_HOST` — external TURN hostname/IP.
- `OXIDESFU_DISCOVERY_EXTERNAL_TURN_UDP_PORT` — external TURN UDP port (optional).
- `OXIDESFU_DISCOVERY_EXTERNAL_TURN_TLS_PORT` — external TURN TLS port (optional).
- `OXIDESFU_DISCOVERY_EXTERNAL_TURN_USERNAME` / `OXIDESFU_DISCOVERY_EXTERNAL_TURN_PASSWORD` — external TURN credentials (optional; defaults to `OXIDESFU_DISCOVERY_TURN_USERNAME/PASSWORD`).

When `OXIDESFU_DISCOVERY_EXTERNAL_TURN_HOST` is set and `OXIDESFU_ICE_SERVERS_JSON` is not already set, the runner synthesizes `OXIDESFU_ICE_SERVERS_JSON` automatically for OxideSFU using `turn:`/`turns:` URLs from these external TURN fields.

The default owned TURN configuration is intentionally test-only: it binds UDP TURN on the configured local host, permits `127.0.0.0/8` restricted peers so same-host relay candidates work, and uses a small relay-port range for local debugging. It does not provide TCP/TLS TURN. The relaxation mirrors the script-managed coturn policy and is not a production default; configure production peer policy and external TCP/TLS TURN explicitly for a deployed service.

### Latest Go SDK evidence (2026-07-13)

Focused server-sdk-go contracts passed against a locally spawned OxideSFU:

- `TestSimulcastCodec`
- `TestE2EE_H264RoundTrip`
- `TestDynacastRepublish`
- `TestRegionFallbackConnects` and `TestRegionFallbackSignalFailure`
- owned-TURN `TestForceTLS` and `TestLimitPayloadSize`

The required unfiltered owned-TURN run did **not** pass. It failed at `TestJoinSinglePeerConnection`: media forwarding completed, but the subscriber did not receive the immediate reliable data packet. Do not promote the Go SDK row to passing until this command exits zero without filters:

```bash
LIVEKIT_API_KEY=devkey \
LIVEKIT_API_SECRET=secret \
OXIDESFU_DISCOVERY_TURN_MODE=on \
OXIDESFU_DISCOVERY_TURN_BACKEND=oxide \
tools/conformance/server-sdk-go-full-suite.sh
```

### Best practice: external TURN vs script-managed local TURN

If you already have a stable external TURN deployment, prefer using it for discovery runs:

```bash
OXIDESFU_DISCOVERY_TURN_MODE=off \
OXIDESFU_DISCOVERY_EXTERNAL_TURN_HOST=turn.example.net \
OXIDESFU_DISCOVERY_EXTERNAL_TURN_UDP_PORT=3478 \
OXIDESFU_DISCOVERY_EXTERNAL_TURN_TLS_PORT=5349 \
OXIDESFU_DISCOVERY_EXTERNAL_TURN_USERNAME=turn-user \
OXIDESFU_DISCOVERY_EXTERNAL_TURN_PASSWORD=turn-pass \
tools/conformance/server-sdk-go-full-suite.sh
```

Use script-managed local TURN (`OXIDESFU_DISCOVERY_TURN_MODE=on`) when you want a fully local reproducible fallback or quick debugging on one machine.

Force OxideSFU's owned runtime explicitly:

```bash
OXIDESFU_DISCOVERY_TURN_MODE=on \
OXIDESFU_DISCOVERY_TURN_BACKEND=oxide \
tools/conformance/server-sdk-go-full-suite.sh
```

If you want strict coturn parity with legacy runs, force:

```bash
OXIDESFU_DISCOVERY_TURN_MODE=on \
OXIDESFU_DISCOVERY_TURN_BACKEND=coturn \
tools/conformance/server-sdk-go-full-suite.sh
```

## `livekit-cli` full suite

```bash
tools/conformance/livekit-cli-full-suite.sh
```

What it does:

- starts or reuses OxideSFU,
- initializes LiveKit CLI submodules if the vendored PortAudio header is missing,
- runs `go test ./... -count=1 -v -p 1` in `livekit-cli`,
- injects `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`, and `LIVEKIT_KEYS`,
- writes a local run log.

### Cloud-dependent upstream test

Upstream `cmd/lk` includes `TestSessionE2E`. In its default form it uses the echo-agent under `cmd/lk/testdata/echo-agent`, which uses hosted/cloud LLM inference. Without valid cloud LLM credentials/runtime, it fails with errors such as `invalid API key` even when OxideSFU signaling/API behavior is fine.

The runner now defaults to skipping only `TestSessionE2E` for a rerunnable local discovery baseline (and keeps the skip visible in logs).

Equivalent explicit command (still supported):

```bash
OXIDESFU_DISCOVERY_GO_TEST_SKIP_PATTERN='TestSessionE2E$' \
tools/conformance/livekit-cli-full-suite.sh
```

The runner prints/logs the built-in reason:

```text
TestSessionE2E depends on external cloud LLM credentials/runtime; it is not purely OxideSFU protocol coverage in this environment
```

If you intentionally provision the cloud LLM dependency, opt into unskipped cloud E2E coverage:

```bash
OXIDESFU_DISCOVERY_LIVEKIT_CLI_INCLUDE_CLOUD_E2E=true \
tools/conformance/livekit-cli-full-suite.sh
```

## `rust-sdks` full suite

```bash
tools/conformance/rust-sdks-full-suite.sh
```

What it does:

- starts or reuses OxideSFU,
- runs `cargo test --workspace --all-targets -- --nocapture` in `rust-sdks`,
- injects `LIVEKIT_URL`, `LIVEKIT_API_KEY`, and `LIVEKIT_API_SECRET`,
- writes a local run log.

Useful overrides:

- `OXIDESFU_DISCOVERY_RUST_TEST_FILTER` — optional positional test filter passed to `cargo test`.
- `OXIDESFU_DISCOVERY_RUST_SDKS_CARGO_TEST_EXTRA_ARGS` — extra cargo test args appended before `-- --nocapture`.



## `client-sdk-js` full suite

```bash
tools/conformance/client-sdk-js-full-suite.sh
```

What it does:

- starts or reuses OxideSFU,
- builds `lk` from `livekit-cli` unless `LK_BIN` is provided,
- mints a short-lived join token,
- installs dependencies with pnpm,
- runs `pnpm test`,
- builds `livekit-client`,
- packs the SDK for smoke-tests,
- runs the Playwright smoke suite,
- writes a local run log.

This script does not require a custom `client-sdk-js` branch. If a checkout contains `smoke-tests/tests/oxidesfu-conformance.spec.ts`, Playwright will run it as part of the smoke suite. If the checkout is clean upstream and does not contain that file, the upstream smoke suite still runs.

Reference copy of the optional OxideSFU browser probe:

- `tools/conformance/reference/client-sdk-js-oxidesfu-conformance.spec.ts`

## `livekit` Go server full suite

### Scoped/default discovery runner

```bash
tools/conformance/livekit-full-suite-all.sh
```

What it does by default (`OXIDESFU_DISCOVERY_LIVEKIT_PACKAGE_SCOPE=external`):

- starts or reuses OxideSFU,
- runs upstream LiveKit `./test` package checks against OxideSFU in external-server mode,
- shards top-level tests across isolated OxideSFU instances by default,
- injects `LIVEKIT_URL`, `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`, and `LIVEKIT_KEYS`,
- writes local aggregate and per-shard logs.

This is the default/scoped discovery gate and works with clean upstream `livekit` checkouts.

**Latest scoped run (2026-07-13):** the default external scope completed all 49
isolated `./test` shards against a locally spawned OxideSFU. **46 passed and 3
failed.** The failures are `TestConnectionStats`,
`TestMultinodeDataPublishing`, and `TestMultinodePublishingUponJoining`.
The two multinode publishing failures are known gaps; `TestConnectionStats`
requires a focused follow-up before it can again be represented as passing.

### Broad all-packages mode (`livekit` integration package included)

Use the same runner with `OXIDESFU_DISCOVERY_LIVEKIT_PACKAGE_SCOPE=all`.

For reproducible broad reruns, use the active fork compatibility branch, which includes the test port overrides:

```bash
git clone --branch oxidesfu/livekit-compat https://github.com/andreeco/livekit.git /path/to/livekit
```

or with an existing checkout:

```bash
cd /path/to/livekit
git remote add andreeco https://github.com/andreeco/livekit.git
git fetch andreeco oxidesfu/livekit-compat
git checkout -B oxidesfu/livekit-compat andreeco/oxidesfu/livekit-compat
```

Then run:

```bash
LIVEKIT_REPO=/path/to/livekit \
OXIDESFU_DISCOVERY_LIVEKIT_PACKAGE_SCOPE=all \
tools/conformance/livekit-full-suite-all.sh
```

The runner uses non-conflicting test ports via env:

- `LK_TEST_SERVER_PORT` (default `17880`)
- `LK_TEST_SERVER_PORT_SECOND` (default `18880`)
- `LK_TEST_TURN_UDP_PORT` (default `19478`)
- `LK_TEST_WEBHOOK_PORT` (default `17890`)
- `LK_TEST_TURN_RESTRICTED_PEER_CIDRS` (default from the OxideSFU runner: `10.0.0.0/8,172.16.0.0/12,192.168.0.0/16`)

`LK_TEST_TURN_RESTRICTED_PEER_CIDRS` is used only by the private `oxidesfu-test-port-overrides` branch to make upstream `TestTurnRelay` portable across common private LAN/Docker ranges. Override the OxideSFU-side value with `OXIDESFU_DISCOVERY_LIVEKIT_TURN_RESTRICTED_PEER_CIDRS` if your local topology needs a narrower or different list.

Use `livekit-full-suite-all.sh` for maximum upstream coverage discovery. Keep promoting relevant findings into internal OxideSFU tests before behavior changes.

## Troubleshooting

### A runner warns about `LIVEKIT_URL=wss://...`

This is expected if your shell has `LIVEKIT_URL` exported. Unless `OXIDESFU_REUSE_SERVER=true`, the runner ignores it and starts local OxideSFU.

### `oxidesfu-server` rejects `--dev`

The conformance scripts do not pass `--dev`; they configure the local API key/secret explicitly.

### `livekit-cli` build fails on ALSA headers

Install:

```bash
sudo apt install -y libasound2-dev
```

### `livekit-cli` needs vendored PortAudio sources

Run:

```bash
cd /home/andre/rustprojects/othercode/livekit-cli
git submodule update --init --recursive
```

or rerun `tools/conformance/livekit-cli-full-suite.sh`, which initializes submodules automatically when the vendored header is missing.

### Use an existing `lk` binary

```bash
LK_BIN=/usr/local/bin/lk tools/conformance/client-sdk-js-full-suite.sh
```

### `server-sdk-go` TURN logs show `CreatePermission error response (error 403)`

For the owned runtime, this means the configured peer CIDR policy rejected the requested peer. The script-managed same-host topology defaults `OXIDESFU_DISCOVERY_TURN_ALLOWED_PEER_CIDRS` to `127.0.0.0/8`; if you override it or change `OXIDESFU_DISCOVERY_TURN_HOST`, include the CIDR containing the relay peers:

```bash
OXIDESFU_DISCOVERY_TURN_MODE=on \
OXIDESFU_DISCOVERY_TURN_BACKEND=oxide \
OXIDESFU_DISCOVERY_GO_TEST_EXTRA_FLAGS='-p 1 -run TestForceTLS$' \
tools/conformance/server-sdk-go-full-suite.sh
```

When using the explicit `coturn` backend, review coturn's peer-policy configuration separately.

### `rust-sdks` full discovery fails in `yuv-sys` with missing files

Initialize submodules in the rust-sdks checkout:

```bash
cd /home/andre/rustprojects/othercode/rust-sdks
git submodule update --init --recursive
```

If you previously ran discovery before submodules were initialized, clean stale build output and rerun:

```bash
cargo clean -p yuv-sys
```

### `rust-sdks` full discovery fails with `Could not find protoc`

Install protobuf compiler:

```bash
sudo apt update
sudo apt install -y protobuf-compiler
```

### `rust-sdks` full discovery fails with `Unable to find libclang`

Install LLVM/Clang development libraries:

```bash
sudo apt install -y clang libclang-dev llvm-dev
```

If `libclang` is installed in a non-standard location, set `LIBCLANG_PATH` to a directory containing `libclang.so` before running discovery.

### `client-sdk-js` runner says `pnpm is required`

Install pnpm, or enable corepack:

```bash
corepack enable
corepack prepare pnpm@latest --activate
```

### `client-sdk-js` Playwright browser missing

Install Chromium for the smoke-tests workspace:

```bash
cd /home/andre/rustprojects/othercode/client-sdk-js
pnpm --dir smoke-tests exec playwright install chromium
```

or rerun `tools/conformance/client-sdk-js-full-suite.sh`, which installs Chromium automatically.

## Promotion rule

External full-suite discovery is a discovery/confidence workflow. Relevant compatibility failures should be promoted into internal `oxidesfu-test` coverage before changing OxideSFU behavior. Unsupported LiveKit features should be tracked in `docs/parity-matrix.md`, not hidden by default skips.
