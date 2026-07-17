# LiveKit configuration compatibility

**Status:** supported migration profile complete as of 2026-07-17.

OxideSFU supports a strict, fail-closed subset of the LiveKit YAML schema. This
is not a claim of full `livekit.yaml` parity: every field outside the supported
profile is rejected at translation time rather than silently ignored.

## Product contract

OxideSFU has two configuration modes:

- **Native mode:** `OXIDESFU_*` environment variables and native CLI options.
- **LiveKit migration mode:** a LiveKit YAML file is checked, translated, or
  used directly with no ambient native overrides.

```sh
# Validate that a YAML file fits the supported profile.
oxidesfu-server config check-livekit /etc/livekit.yaml

# Emit a redacted review-oriented native configuration summary.
oxidesfu-server config translate-livekit /etc/livekit.yaml

# Start directly from the same strict translation path.
oxidesfu-server --livekit-config /etc/livekit.yaml
```

Migration mode does not merge `OXIDESFU_*` values with YAML. This keeps the
result deterministic and prevents an operator from believing a YAML field was
applied when a native override changed the runtime behavior.

## Reference baseline

- LiveKit server: `ae09b7d0ad94d764f0c97d183efd36476163e819`.
- Upstream YAML shape: `livekit/config-sample.yaml`.
- Translation boundary: `crates/oxidesfu-core/src/config/livekit_yaml.rs`.
- Runtime construction: `crates/oxidesfu-server/src/main.rs` and
  `crates/oxidesfu-server/src/config.rs`.
- Current broader compatibility register: `docs/gaps.md`.

## Supported migration profile

| LiveKit YAML field | Status | Runtime contract |
|---|---|---|
| `port` | Translated | HTTP/WebSocket bind port. |
| `keys` | Translated | Multiple YAML keys are loaded into the API-key verifier; generated output never prints secrets. |
| `redis.address` | Translated | Uses the Redis room-node directory and relay runtime. |
| `rtc.port_range_start/end`, `rtc.udp_port`, `rtc.tcp_port` | Translated | Configures RTC listener ports. |
| `rtc.use_external_ip`, `rtc.node_ip` | Translated | Explicit node IP is used when present; otherwise startup STUN discovery resolves the external address. |
| `rtc.stun_servers` | Translated | Advertised in join and reconnect ICE configuration. |
| `rtc.turn_servers` static credentials | Translated | `udp`, `tcp`, and `tls` URL forms plus static username/credential are advertised. |
| `rtc.allow_tcp_fallback`, `rtc.tcp_fallback_rtt_threshold`, `rtc.allow_udp_unstable_fallback` | Translated | Configures the implemented fallback policy. |
| `turn.enabled`, UDP port/range/domain/TTL/peer CIDRs | Translated | Configures Oxide-owned UDP TURN. |
| `room.auto_create` | Translated | Controls implicit room creation on join. |
| `room.max_participants`, `room.empty_timeout`, `room.departure_timeout` | Translated | Defaults for auto-created and RoomService-created rooms when the request omits a value; the participant cap is enforced before a new identity joins. |
| `webhook.api_key`, `webhook.urls` | Translated | Configures existing webhook dispatch. |
| `region`, supported `node_selector` settings | Translated | Configures placement and relay ownership. |

## Explicitly rejected fields

The checker and startup mode reject unsupported fields, including:

- Redis Sentinel, Cluster, TLS, and certificate topology.
- Dynamic external TURN credentials (`rtc.turn_servers.secret`,
  `secret_file`, `ttl`); participant-specific HMAC issuance is not implemented
  for external TURN.
- Owned TURN/TLS listener fields (`turn.tls_port`, `external_tls`, certificate
  and key fields). An external `turns:` server may be advertised, but Oxide does
  not provide a TURN-over-TLS listener.
- RTC candidate filters, ICE lite, mDNS/loopback policy, congestion-control and
  packet-buffer tuning.
- Dedicated Prometheus/pprof listeners and LiveKit logging configuration.
- Room codec policy, remote unmute, playout delay, stream sync, and remaining
  `limit.*` policy fields.
- Ingress, egress, RTMP/WHIP, Redis HA, PSRPC, and LiveKit signal-relay tuning.

A rejected field is a safe migration failure, not a partial success.

## Proven runtime contracts

The supported profile is backed by focused tests and process checks:

- YAML process startup plus `lk room create/list/delete`.
- Redis-backed YAML process startup, RoomService lifecycle, and Rust SDK join.
- Static external TURN YAML translation and exact SDK `JoinResponse` ICE entry.
- Two native processes sharing Redis: a client joins through a non-owning node,
  receives the selected owner’s ICE configuration, and uses RoomService
  list/get through the origin.
- External-IP candidate address mapping and source-bound STUN discovery.
- Room default propagation through YAML translation, server state, auto-created
  rooms, RoomService creates, and participant-cap enforcement.

Relevant commits: `248d3dd3`, `f8ad87db`, `863b5869`, `a069f13d`,
`88089101`, `a221cc0d`, `6fa6933d`, and `cb53ddf2`.

## Deployment requirements

A supported deployment needs:

1. direct publication of RTC UDP and ICE/TCP ports; do not put them behind an
   HTTP reverse proxy;
2. Redis when running a distributed room-node deployment;
3. either Oxide-owned UDP TURN or an independently operated external TURN
   server;
4. explicit API credentials provided through deployment-safe secret handling;
5. an external validation run, not only container-local health checks.

Before production, validate `/healthz`, `/readyz`, and `/metrics`; use `lk` with
an explicit target URL; join independent clients; and exercise the intended
UDP/TCP/TURN path from a separate host, VM, or network namespace. Docker bridge
hairpin behavior is not evidence of public TURN reachability.

## Phase status

| Phase | Status | Evidence |
|---|---|---|
| Strict input boundary and supported migration profile | Complete | Private YAML adapter, normalized `ServerConfig`, and fail-closed parser tests. |
| Checker, translator, and YAML startup | Complete | `check-livekit`, redacted `translate-livekit`, and `--livekit-config` use one translation path. |
| Supported single-node and Redis runtime behavior | Complete | CLI, SDK, Redis, TURN-advertisement, external-IP, and room-policy process/unit contracts. |
| Distributed relay behavior | Complete for the supported profile | Two-process Redis test proves owner-resolved signaling and management-plane forwarding. |
| Full LiveKit YAML parity | Not a finite phase | Unsupported systems remain separate compatibility projects, listed below. |

## Current implementation backlog

The next work is not legacy cleanup; it is new compatibility scope. Each item
requires a pinned upstream reference map and a failing behavior test before
implementation.

1. **Public-network external-IP validation:** validate mapped candidates from a
   separate host and add per-interface discovery for wildcard RTC binds.
2. **External dynamic TURN credentials:** implement and test participant-specific
   HMAC credentials for `rtc.turn_servers.secret` and `secret_file`, or retain
   fail-closed behavior.
3. **Room and server policy:** add individual `limit.*`, codec, remote-unmute,
   playout-delay, or stream-sync fields only with full runtime enforcement.
4. **Deployment topology:** Redis HA/TLS and owned TURN/TLS are distinct
   transport/topology projects.
5. **Media services:** ingress, egress, RTMP, and WHIP require dedicated runtime
   implementations and are not YAML-adapter work.

## Conclusion

The original configuration migration phases are complete for the supported
profile. OxideSFU now provides deterministic YAML checking and startup with
proven single-node, Redis, relay, external TURN advertisement, external-IP, and
room-default behavior. It intentionally does not claim full LiveKit YAML
compatibility; the remaining items are explicit, independently scoped features.
