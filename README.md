# OxideSFU


OxideSFU is a personal _unfinished_ Rust implementation of LiveKit. It should work with the LiveKit ecosystem (SDKs, CLI). The name is not yet finalized.

This project is **not affiliated with LiveKit**.

**The project was developed primarily by LLM agents, constrained by TDD, source inspection, compatibility tests, and differential checks against LiveKit behavior.** Some portions may be adapted from Apache-2.0-licensed LiveKit source; see [`NOTICE`](NOTICE) and [`docs/provenance.md`](docs/provenance.md). This is important context so you know what to expect from this project.

This project hopefully helps develop and debug WebRTC implementations in Rust. It also helps benchmark against official LiveKit to find performance gaps.

**Don't expect that I will further develop this project.** Ideally, LiveKit developers or someone else from the Rust community who sees value in the code picks up this project and finishes it well. This publishing may be a handsoff to someone else.

## Benchmarks

Run the full comparison against upstream Go LiveKit from the repository root. Five runs per scenario are recommended for stable results:

```bash
OXIDESFU_ENABLE_BENCHMARKS=1 OXIDESFU_BENCHMARK_MODE=full OXIDESFU_BENCHMARK_RUNS=1 \
  cargo test -p oxidesfu-test benchmark_ -- --nocapture
```

Results are written to `target/benchmarks/`. See the [benchmark guide](crates/oxidesfu-test/src/benchmark/README.md) for prerequisites and regression controls.

## Conformance status

OxideSFU keeps external compatibility checks in `tools/conformance/` and runs them against upstream-style SDK and server checkouts. These are black-box discovery checks; the OxideSFU Rust tests remain the required regression suite.

| Surface | Latest local result (2026-07-13) | Notes |
| --- | --- | --- |
| Rust SDK | ✅ Passed | `rust-sdks-full-suite.sh`; the complete workspace and all targets passed against a local OxideSFU. |
| LiveKit CLI | ✅ Passed | `livekit-cli-full-suite.sh`. |
| JavaScript SDK | ✅ Passed | `client-sdk-js-full-suite.sh`. |
| Go server SDK | ❌ Owned-TURN full run blocked | Focused codec, E2EE H.264, dynacast, region-fallback, ForceTLS, and payload contracts pass. The unfiltered owned-TURN run fails at `TestJoinSinglePeerConnection` because the subscriber misses its immediate reliable data packet. See the [investigation](docs/go-sdk-single-pc-reliable-data-investigation.md). |
| Native upstream LiveKit ports | ⚠️ 51 / 52 passed | Full serial run on 2026-07-13. `TestDataPublishSlowSubscriber` failed its above-threshold contiguity assertion twice; it is timing-sensitive and remains under investigation. |
| LiveKit external contracts | 47 / 49 passed | Latest full local run on 2026-07-13. `TestMultinodeDataPublishing` and `TestMultinodePublishingUponJoining` remain reproducible failures. |

The owned TURN runtime is externally covered by `TestTurnRelay/allow`, `TestTurnRelay/not-allowed`, `TestTurnRelay/denied-overrides-allowed`, and `TestTurnAuthFailure`. The focused Go SDK owned-TURN probe (`TestForceTLS` and `TestLimitPayloadSize`) passed on 2026-07-13; it still observed bounded relay allocation `486 Allocation Quota Reached` responses while host candidate pairs connected. Do not treat this as general owned-TURN robustness evidence until the unfiltered Go SDK suite passes.



The canonical named external-contract inventory and its evidence live in [`crates/oxidesfu-test/src/upstream_livekit/README.md`](crates/oxidesfu-test/src/upstream_livekit/README.md#known-external-contract-inventory). Run instructions and per-run log locations are in [`tools/conformance/README.md`](tools/conformance/README.md).

_Passing tests sadly do not mean that everything works as expected._

## Devcontainer + required sibling repositories

If you use the devcontainer, read `.devcontainer/README.md` first.
It includes an explicit source map for required sibling repositories used by `tools/conformance/*`.

Current recommended forks/branches for compatibility work:

- `../webrtc` → `git@github.com:andreeco/webrtc.git` on branch `oxidesfu/webrtc-compat`
  (Cargo fetches its `webrtc`, `rtc`, and `rtc-stun` crates from this branch; `rtc` is not a submodule.)
- `../othercode/livekit` → `git@github.com:andreeco/livekit.git` on branch `oxidesfu/livekit-compat`

Other sibling repos (`livekit-cli`, `server-sdk-go`, `rust-sdks`, `client-sdk-js`) can be cloned from upstream defaults unless you need custom patches.

## Quickstart

### Built-in TURN runtime

OxideSFU can run its own UDP TURN runtime; do **not** start coturn for this
local setup. The runtime issues participant-specific TURN credentials in the
signaling response.

```bash
# These are development-only credentials. Use distinct secrets outside local testing.
export LIVEKIT_API_KEY=devkey
export LIVEKIT_API_SECRET=secret

OXIDESFU_TURN_ENABLED=true \
OXIDESFU_TURN_DOMAIN=127.0.0.1 \
OXIDESFU_TURN_BIND=127.0.0.1 \
OXIDESFU_TURN_UDP_PORT=3479 \
OXIDESFU_TURN_RELAY_PORT_RANGE_START=31000 \
OXIDESFU_TURN_RELAY_PORT_RANGE_END=31050 \
cargo run -p oxidesfu-server -- \
  --bind 127.0.0.1:7880 \
  --api-key "$LIVEKIT_API_KEY" \
  --api-secret "$LIVEKIT_API_SECRET"
```

The owned runtime currently provides UDP TURN. Configure an external TURN
service separately when TCP/TLS TURN is required.

```
lk token create \
  --api-key "$LIVEKIT_API_KEY" \
  --api-secret "$LIVEKIT_API_SECRET" \
  --identity admin-user \
  --room test-room \
  --join \
  --admin \
  --valid-for 24h \
  --token-only
```

```
lk token create \
  --api-key "$LIVEKIT_API_KEY" \
  --api-secret "$LIVEKIT_API_SECRET" \
  --identity admin-user2 \
  --room test-room \
  --join \
  --admin \
  --valid-for 24h \
  --token-only
```

```
# https://meet.livekit.io/?tab=custom
ws://127.0.0.1:7880 # not wss://
```
