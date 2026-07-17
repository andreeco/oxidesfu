# OxideSFU


OxideSFU is a personal _unfinished_ Rust implementation of LiveKit. It should work with the LiveKit ecosystem (SDKs, CLI). The name is not yet finalized.

This project is **not affiliated with LiveKit**.

**The project was developed primarily by LLM agents, constrained by TDD, source inspection, compatibility tests, and differential checks against LiveKit behavior.** This is important context so you know what to expect from this project.

**Don't expect that I will further develop this project.** Ideally, some gifted developers from the Rust community see value in the code and pick it up and finish this project well. This publishing may be a handoff to someone else.

## Conformance status

OxideSFU keeps external compatibility checks in `tools/conformance/` and runs them against upstream-style SDK and server checkouts. There is also a high internal test coverage. 

There may be some flacky tests remaining but in general (nearly) all internal and external tests should pass.

_Passing tests sadly do not mean that everything works as expected._

## Quickstart

### Built-in TURN runtime

OxideSFU can run its own UDP TURN runtime; do **not** start coturn for this
local setup. The runtime issues participant-specific TURN credentials in the
signaling response.

### Start the server
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
RUST_LOG=oxidesfu_signaling=debug,oxidesfu_rtc=debug \
cargo run -p oxidesfu-server -- \
  --bind 127.0.0.1:7880 \
  --api-key "$LIVEKIT_API_KEY" \
  --api-secret "$LIVEKIT_API_SECRET"
```

The owned runtime currently provides UDP TURN. Configure an external TURN
service separately when TCP/TLS TURN is required.

## Create access tokens with `lk`

Run each command and save the token it prints:

```bash
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

```bash
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

### Run two browser windows

In each browser window, open [the LiveKit Meet custom connection page](https://meet.livekit.io/?tab=custom).

Enter the local WebSocket URL `ws://127.0.0.1:7880` (not `wss://127.0.0.1:7880`) and the corresponding access token generated above. Use a different token in each window.

## License

OxideSFU is currently published under the [Apache License, Version 2.0](LICENSE-APACHE). I may consider publishing future versions under the MIT License if there are no legal restrictions. See also [`NOTICE`](NOTICE) and [`docs/provenance.md`](docs/provenance.md).
