# Go SDK single-PC immediate reliable-data investigation

## Status

The upstream Go SDK's full suite remains blocked in
`TestJoinSinglePeerConnection` when run against local OxideSFU with its owned
TURN runtime. The subscriber receives media, but not the publisher's immediate
reliable data packet.

This is an upstream SDK/conformance investigation, **not** a claim that
OxideSFU is conformant or that the Go test is invalid.

## Reproduction

```sh
LIVEKIT_API_KEY=devkey \
LIVEKIT_API_SECRET=secret \
OXIDESFU_DISCOVERY_TURN_MODE=on \
OXIDESFU_DISCOVERY_TURN_BACKEND=oxide \
OXIDESFU_DISCOVERY_GO_TEST_EXTRA_FLAGS='-p 1 -run TestJoinSinglePeerConnection$' \
tools/conformance/server-sdk-go-full-suite.sh
```

The same failure occurred in the latest unfiltered owned-TURN Go SDK run.

## Observed boundary

Instrumented OxideSFU tracing showed that both peers' `_reliable` and `_lossy`
data channels registered successfully. The publisher's immediate reliable
packet did not reach OxideSFU's data-channel receive loop, which rules out
recipient resolution and server-side relay as the immediate cause for the
observed run.

## Client comparison

`TestJoinSinglePeerConnection` was introduced in upstream `server-sdk-go`
commit `0e6fe565`. It waits for the publisher's `OnParticipantConnected`
callback and then calls:

```go
pub.LocalParticipant.PublishDataPacket(
    UserData([]byte("singlepc")),
    WithDataPublishReliable(true),
)
```

The test discards the returned error.

At the inspected revisions:

| Client | Revision | Data-send readiness behavior |
| --- | --- | --- |
| Rust SDK | `549d9f1` | Waits for the requested publisher data channel to be open. |
| JavaScript SDK | `af9376d` | Waits for ICE connectivity and for the requested data channel's `readyState` to be `open`. |
| Go SDK | `16c9237` | Its publisher-primary path can return after peer-connection connectivity; `publishDataPacket` ignores the result of `dc.Send(data)`. |

The Go path is therefore sensitive to participant-update and SCTP
data-channel-open ordering in a way that the Rust and JavaScript clients are
not.

## Open question

Run this focused test against the official LiveKit server before assigning the
compatibility responsibility:

- If LiveKit reliably delays the relevant participant/update ordering until the
  Go data channel is usable, OxideSFU should reproduce that observable
  guarantee.
- Otherwise, the Go SDK should wait for data-channel readiness and handle send
  failures instead of discarding them.
