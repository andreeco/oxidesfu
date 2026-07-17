# LiveKit configuration compatibility: scope, policy, and plan

**Status:** active implementation note.

Implemented so far:

- strict LiveKit YAML parser/translator in
  `crates/oxidesfu-core/src/config/livekit_yaml.rs`;
- `oxidesfu-server config check-livekit <path>`;
- `oxidesfu-server config translate-livekit <path>`;
- opt-in startup `oxidesfu-server --livekit-config <path>`;
- fail-closed rejection for unsupported fields instead of silent drops.

Open: broad deployment/runtime parity for unsupported or `different` fields.

## Decision to make

OxideSFU's goal is LiveKit ecosystem compatibility. That must be split into
three independently testable claims:

1. **Wire/runtime compatibility** — a standard LiveKit client, SDK, CLI, or
   Twirp caller can use implemented behaviour after OxideSFU starts.
2. **Deployment compatibility** — an operator can expose the required HTTP,
   WebSocket, ICE/TCP, WebRTC/UDP, TURN, Redis, proxy, secret, and health-check
   surfaces safely.
3. **Configuration compatibility** — an existing LiveKit configuration can be
   assessed and, where supported, translated without silently changing
   behaviour.

Starting a server successfully proves none of these claims by itself. A server
can start with an ignored codec policy, inaccessible relay port range, omitted
TURN/TLS listener, unintended room limits, or incompatible Redis topology.

The recommended product position is:

> OxideSFU should aim for **fail-closed, evidence-backed LiveKit configuration
> migration compatibility**, not initially for transparent acceptance of every
> LiveKit `livekit.yaml` setting.

In other words, accepting a LiveKit YAML file is valuable only if every
behaviour-affecting field is either:

- translated to a tested OxideSFU semantic equivalent,
- explicitly reported as a known behavioural difference with operator opt-in,
  or
- rejected before the server starts.

Ignoring fields is not compatibility.

## Reference map

This note is based on the following checked-out sources and commits:

- LiveKit server compatibility reference:
  `livekit` at `ae09b7d0ad94d764f0c97d183efd36476163e819`.
- LiveKit configuration source:
  [`config-sample.yaml`](../../../livekit/config-sample.yaml).
- LiveKit deployment reference:
  [`deploy/README.md`](../../../livekit/deploy/README.md). The upstream server
  repository has no official Docker Compose file; it publishes a Docker image,
  VM guidance, and Helm-based Kubernetes deployment instead.
- OxideSFU configuration implementation:
  [`crates/oxidesfu-core/src/config.rs`](../../crates/oxidesfu-core/src/config.rs).
- OxideSFU HTTP server/router:
  [`crates/oxidesfu-server/src/app.rs`](../../crates/oxidesfu-server/src/app.rs).
- OxideSFU TURN runtime:
  [`crates/oxidesfu-server/src/turn_runtime.rs`](../../crates/oxidesfu-server/src/turn_runtime.rs).
- Existing Compose deployments:
  [`compose.yaml`](../../compose.yaml) and
  [`deploy/compose.remote-livekit-name.yaml`](../../deploy/compose.remote-livekit-name.yaml).
- Current compatibility caveats:
  [`../gaps.md`](../gaps.md).

## Configuration architecture decision

OxideSFU remains its own project. Its internal configuration model—not the
LiveKit YAML schema—must remain the runtime source of truth.

```text
LiveKit YAML                         OxideSFU environment / CLI
     |                                          |
     v                                          v
LiveKit compatibility adapter              native input loader
     |                                          |
     +----------> normalized OxideSFU configuration <----------+
                                      |
                                      v
                           validation and startup
```

The compatibility adapter is an input boundary. It must not leak LiveKit YAML
conditionals throughout signalling, API, RTC, TURN, or room code. A clean
module split is:

```text
config/
  model.rs          normalized OxideSFU configuration types
  native.rs         OXIDESFU_* environment and native CLI parsing
  validate.rs       source-independent configuration validation
  livekit_yaml.rs   private LiveKit YAML parsing, classification, and translation
```

The exact file layout may differ from this sketch, but the dependency direction
must not: both input formats converge before runtime construction.

### Input modes and precedence

Keep native startup as the default:

```sh
oxidesfu-server
```

This reads the existing `OXIDESFU_*` environment variables and native command
line flags.

Compatibility tooling (implemented):

```sh
oxidesfu-server config check-livekit /etc/livekit.yaml
oxidesfu-server config translate-livekit /etc/livekit.yaml
```

Opt-in startup mode (implemented):

```sh
oxidesfu-server --livekit-config /etc/livekit.yaml
```

Do not combine a LiveKit YAML file with arbitrary `OXIDESFU_*` overrides by
accident. Ambiguous precedence makes migration results unreproducible. The
initial policy should be one source at a time:

- native mode: environment plus native CLI precedence, as today;
- compatibility mode: LiveKit YAML only;
- an explicit future `--set`/native override mechanism may be added only when
  each override is reported in the migration output and tested.

The adapter needs a declared upstream-schema baseline. Initially that is the
LiveKit sample/configuration model at
`ae09b7d0ad94d764f0c97d183efd36476163e819`; later support changes must name
the exact upstream revision they add or change.

## Current configuration model

LiveKit conventionally starts with a YAML configuration file, often named
`livekit.yaml`:

```sh
livekit-server --config /etc/livekit.yaml
```

That YAML contains nested configuration such as `redis`, `rtc`, `turn`,
`webhook`, `room`, `limit`, and `node_selector`.

OxideSFU currently accepts a distinct configuration model:

- `OXIDESFU_*` environment variables;
- matching command-line arguments for much of the supported surface;
- file-backed single API key and secret inputs for Docker secrets.

Its `--config` option is not a parser for the LiveKit YAML schema. Therefore a
former deployment's Compose file cannot retain a mounted `livekit.yaml` and
expect the OxideSFU image to apply it.

## Compatibility policy

### Near-term policy: migration checker and translator

Implement the explicit checker and translator described in
[Input modes and precedence](#input-modes-and-precedence). Exact command
spelling can evolve, but normal OxideSFU startup must not ambiguously mix two
unrelated configuration models.

The checker must classify every supplied field as one of:

| Classification | Meaning | Startup policy |
|---|---|---|
| `translated` | A tested semantic equivalent exists. | Safe to emit OxideSFU configuration. |
| `different` | OxideSFU has related behaviour but not the same semantics. | Fail by default; allow only explicit opt-in with a clear warning. |
| `unsupported` | No runtime feature exists. | Fail. |
| `unknown` | The parser does not recognize the YAML key. | Fail. |

Example output:

```text
translated: rtc.port_range_start -> OXIDESFU_RTC_UDP_PORT_RANGE_START
translated: rtc.port_range_end -> OXIDESFU_RTC_UDP_PORT_RANGE_END
unsupported: turn.cert_file (owned TURN/TLS listener is not implemented)
unsupported: ingress.rtmp_base_url (RTMP ingress runtime is not implemented)
different: room.departure_timeout (OxideSFU empty-room cleanup has different lifecycle semantics)
```

A `--allow-differences` mode may emit output only after showing each difference.
It must never quietly drop a field.

### YAML startup policy (current)

`--livekit-config` is available now and runs through the same strict
translation and validation path as `check-livekit`.

It must continue to reject unsupported/different fields by default and must not
claim arbitrary upstream YAML support.

## Configuration surface map

The following table describes the current state against the referenced
LiveKit sample. It is a planning map, not proof of complete feature parity.
Each `translated` entry needs a parser + translation test and a runtime test
before being claimed as compatibility support.

| LiveKit field | Proposed disposition | Current OxideSFU setting or reason |
|---|---|---|
| `port` | Translate | `OXIDESFU_BIND=0.0.0.0:<port>`; bind host is an OxideSFU deployment choice. |
| `keys` | Different / partial | OxideSFU deployment currently exposes singular `OXIDESFU_API_KEY` and `OXIDESFU_API_SECRET` inputs. Do not flatten a multi-key map until multi-key loading and JWT/Twirp selection have contracts. |
| `redis.address` | Translate | `OXIDESFU_ROOM_NODE_DIRECTORY_BACKEND=redis` plus `OXIDESFU_REDIS_URL=redis://<address>`. |
| Redis DB, username, password encoded in URI | Translate only when URI conversion is lossless | Emit one Redis URI; redact secrets from output/logs. |
| Redis Sentinel, cluster, TLS, CA/client certificate settings | Unsupported initially | OxideSFU has one Redis URL setting, not LiveKit's topology/TLS schema. Validate client capabilities before designing support. |
| `rtc.port_range_start/end` | Translate | `OXIDESFU_RTC_UDP_PORT_RANGE_START/END`; Compose/firewall must expose exactly the same inclusive range. |
| `rtc.udp_port` | Translate | `OXIDESFU_RTC_UDP_PORT`. |
| `rtc.tcp_port` | Translate | `OXIDESFU_RTC_TCP_PORT`; publish directly, never behind an HTTP reverse proxy. |
| `rtc.use_external_ip` | Different | OxideSFU supports `OXIDESFU_RTC_USE_EXTERNAL_IP`, but remote Docker deployments should explicitly set `OXIDESFU_RTC_NODE_IP` to the public IP and prove candidates from outside the host. |
| `rtc.node_ip` | Translate with validation | `OXIDESFU_RTC_NODE_IP`; reject non-IP values and require compatible `use_external_ip` semantics. |
| `rtc.stun_servers` | Translate | Emit `OXIDESFU_ICE_SERVERS_JSON`. |
| `rtc.turn_servers` | Translate with validation | Static `host`/`port`/`protocol` (`udp`, `tcp`, `tls`) plus username/credential translate into advertised ICE servers. Dynamic `secret`, `secret_file`, and `ttl` credentials fail closed until participant-specific credential generation exists. |
| `rtc.allow_tcp_fallback` | Translate | `OXIDESFU_RTC_ALLOW_TCP_FALLBACK`. |
| `rtc.tcp_fallback_rtt_threshold` | Translate | `OXIDESFU_RTC_TCP_FALLBACK_RTT_THRESHOLD_MS`. |
| `rtc.allow_udp_unstable_fallback` | Translate | `OXIDESFU_RTC_ALLOW_UDP_UNSTABLE_FALLBACK`. |
| Candidate filters, `advertise_internal_ip`, `skip_external_ip_validation`, `use_ice_lite`, loopback candidates, mDNS, strict ACKs | Unsupported | They require candidate-gathering/ICE behaviour work, not an environment-variable alias. |
| Congestion control, packet buffers, PLI throttle, batch I/O, data-channel buffered amount | Unsupported / different | Do not expose knobs until their exact behavioural model and test contracts are defined. |
| `prometheus_port` | Different | OxideSFU serves `/metrics` on the main HTTP listener. A dedicated metrics listener is a separately scoped, feasible feature. |
| `debug_handler_port` / Go pprof | Unsupported | OxideSFU exposes `/debug/forwarding-snapshots`, not Go pprof/debug endpoints. |
| `logging.*` | Different | Use `RUST_LOG`; no LiveKit YAML logging, Pion logging, JSON mode, or sampling equivalence currently exists. |
| `room.auto_create` | Translate | `OXIDESFU_ROOM_AUTO_CREATE`. |
| `room.empty_timeout`, `room.departure_timeout` | Different | OxideSFU has cleanup interval and empty-room maximum age. Map only after a lifecycle contract proves equivalent semantics. |
| Room max participants, default codec policy, remote unmute, playout delay, stream sync | Unsupported | These require room/publish/subscribe runtime policies, not just parsing. |
| `webhook.api_key`, `webhook.urls` | Translate | `OXIDESFU_WEBHOOK_API_KEY`, `OXIDESFU_WEBHOOK_URLS`; test signature/event parity independently. |
| `signal_relay.*`, `psrpc.*` | Unsupported / internal difference | OxideSFU has its own relay mechanism. Never pretend its tuning is equivalent without resilience and delivery contracts. |
| `audio.*` | Unsupported | Active-speaker and RED settings have no configuration equivalence. |
| `turn.enabled` | Translate | `OXIDESFU_TURN_ENABLED`. |
| `turn.udp_port` | Translate | `OXIDESFU_TURN_UDP_PORT`. |
| `turn.relay_range_start/end` | Translate | `OXIDESFU_TURN_RELAY_PORT_RANGE_START/END`. |
| `turn.domain` | Translate | `OXIDESFU_TURN_DOMAIN`. |
| `turn.ttl_seconds` | Translate | `OXIDESFU_TURN_CREDENTIAL_TTL_SECONDS`. |
| TURN restricted/denied peer CIDRs | Translate | `OXIDESFU_TURN_ALLOW_RESTRICTED_PEER_CIDRS` and `OXIDESFU_TURN_DENY_PEER_CIDRS`. |
| `turn.tls_port` | Different | OxideSFU can advertise an external `turns:` endpoint but its owned runtime is UDP TURN. It does not thereby provide a TLS TURN listener. |
| `turn.external_tls`, `turn.cert_file`, `turn.key_file` | Unsupported | Requires TURN-over-TCP/TLS listener and certificate/termination architecture. |
| `ingress.rtmp_base_url`, `ingress.whip_base_url` | Unsupported | Twirp ingress paths are incremental; no RTMP/WHIP worker runtime is a deployable replacement. |
| `region` | Translate | `OXIDESFU_REGION`. |
| `node_selector.kind`, sort, algorithm, regions, load limits | Translate only with selector contracts | OxideSFU has related selector settings; cluster placement/relay gaps remain high risk. |
| `limit.*` | Unsupported | Track, bandwidth, subscription, metadata, attribute, room-name, and identity limits need dedicated enforcement and API contracts. |

## Deployment consequences

A Compose project is compatible only when both the container topology and its
server configuration are compatible.

### Basic single-node target

A realistic first migration target contains:

- one OxideSFU service;
- a normal standalone Redis service;
- a reverse proxy that terminates HTTP/TLS and routes to the signalling port;
- direct host publication for ICE/TCP and the RTC UDP range;
- either OxideSFU-owned UDP TURN or an independently operated external TURN;
- one static API credential pair or an explicitly implemented multi-key
  alternative;
- no ingress, egress, SIP, RTMP, WHIP/WHEP, Redis HA, or special LiveKit
  runtime tuning.

The existing [`compose.yaml`](../../compose.yaml) and
[`deploy/compose.remote-livekit-name.yaml`](../../deploy/compose.remote-livekit-name.yaml)
are examples of this narrow topology. The remote file deliberately retains the
`livekit` service name so a Caddy upstream at `livekit:7885` can remain stable.

### Required external validation

For every translated configuration, validation must use the advertised public
surface, not only container-local health checks:

1. Run `docker compose config` on generated output.
2. Start the deployment and verify `/healthz`, `/readyz`, and `/metrics`.
3. Use `lk` with an explicit local URL to create/list/delete a room.
4. Join two independent LiveKit clients with minted JWTs.
5. Verify UDP media, ICE/TCP fallback where configured, and data channels.
6. Run forced-relay TURN validation from a second LAN device, VM, or host; do
   not treat Docker bridge loopback hairpin behaviour as remote TURN proof.
7. Run targeted Rust/Go SDK and browser contracts for every setting that
   changes signalling or media behaviour.

## Implementation plan

### Phase 0 — freeze the boundary and contract ✅

Before implementation:

1. Keep this note and its exact upstream reference revision current.
2. Separate native input parsing, normalized configuration, and runtime
   validation only as far as needed to make both input formats converge.
3. Define a machine-readable support matrix owned by importer tests, not only
   this documentation table.
4. Select the initial supported subset: basic port, static keys, standalone
   Redis, UDP/TCP RTC ports, static ICE servers, owned UDP TURN, webhooks,
   region, and basic node selector settings.
5. Explicitly exclude TLS TURN, Redis HA, ingress/egress, room defaults/limits,
   and advanced RTC controls.

### Phase 1 — parser and fail-closed report ✅

1. Add a private `LiveKitConfigYaml` deserialization model covering the full
   upstream YAML surface needed for classification.
2. Preserve field paths and source locations in diagnostics where practical.
3. Translate into the private normalized configuration type, never directly
   into process environment variables.
4. Reject unknown and unsupported fields. Require an explicit
   `--allow-differences` choice for fields classified as `different`.
5. Redact all secrets in reports, errors, snapshots, and generated output.
6. Add fixture tests for translated, different, unsupported, unknown, malformed,
   upstream-version mismatch, and secret-redaction cases.

This phase is useful even before new server behaviour: it prevents unsafe
migrations and tells an operator exactly why a project cannot move yet.

### Phase 2 — deterministic single-node translation 🚧 (management-plane proof complete)

1. Implement a translation output suitable for Compose, such as an escaped
   `.env` file or structured JSON; do not use shell interpolation as the API.
2. Add black-box tests that start OxideSFU from the generated normalized
   configuration.
3. Test exact ports, public candidate addresses, JWT credentials, Twirp
   endpoints, Redis connection, and TURN credential advertisement.
4. Add a redacted real-world Compose plus `livekit.yaml` fixture only when it
   contains no secret material.
5. Document one known-good generated Compose deployment and its limits.

### Phase 3 — opt-in YAML startup mode ✅ (initial)

1. Add `--livekit-config` only after Phases 1 and 2 are green.
2. Reuse the checker and normalized-model validation exactly; do not create a
   second parsing path for startup.
3. Initially prohibit unreported native environment overrides in this mode.
4. Print the source schema revision and the translated/different field summary
   at startup without printing secret values.

### Phase 4 — expand only with behaviour tests 🚧

Each newly supported LiveKit YAML field needs:

1. an upstream-source reference map at a pinned LiveKit revision;
2. a failing configuration and runtime contract first;
3. the smallest Rust-native implementation;
4. an update to the support matrix and migration diagnostics;
5. targeted tests plus relevant SDK/browser/conformance coverage.

High-value follow-up slices are: multi-key API credentials, dedicated metrics
listener, explicit room lifecycle policy, and selected node-selector semantics.
TURN/TLS, Redis HA, and ingress/egress are separate larger projects.

## Bit-by-bit execution roadmap (next slices)

The strategy is to run and close one migration/runtime slice at a time, each
with tests and conformance evidence before moving on.

### Slice A — external-IP YAML compatibility ✅

**Current blocker discovered:**

- Running `oxidesfu-server config check-livekit ../othercode/livekit/config-sample.yaml`
  fails because LiveKit permits `rtc.use_external_ip: true` without explicit
  `rtc.node_ip`, while OxideSFU currently requires fixed `rtc_node_ip`.

**Work:**

1. Implement external-IP discovery path for YAML/native configs where
   `use_external_ip=true` and `node_ip` is absent.
2. Keep explicit `node_ip` as deterministic override.
3. Add startup diagnostics and failure classification when discovery fails.
4. Add unit tests plus integration coverage for candidate advertisement.

**Completed:** the upstream sample passes the checker; explicit `node_ip` is
preserved as the override; missing node IP performs startup STUN discovery;
and host ICE candidates advertise the configured/discovered external address
while retaining their local socket address for packet routing. See the active
record below for commits and remaining multi-interface/network validation.

### Slice B — real process + YAML + `lk` room API smoke ✅

**Work:**

1. Add process-harness support to start `oxidesfu-server --livekit-config <tmp>`.
2. Add black-box test: healthz + `lk room create/list/delete` against the YAML
   startup process.

**Completed:** `crates/oxidesfu-test/src/harness/livekit_yaml_process.rs`
writes a secret-free temporary fixture, starts the compiled server with
`--livekit-config`, waits for health readiness, and uses `lk` with explicit
URL/key/secret to create, list, and delete a room.

### Slice C — Redis + YAML single-node deployment contract ✅

**Work:**

1. Add YAML fixture with `redis.address` and API keys.
2. Prove room create/list/delete and join path through Redis-backed mode.

**Completed:** `livekit_yaml_redis_process_supports_room_api_and_join` starts a
real local Redis instance, starts the binary from a YAML `redis.address`
fixture, proves RoomService create/list/delete, and connects a Rust SDK
participant through the Redis-backed signalling path.

### Slice D — external TURN mapping for deployment parity ✅

**Completed:** `rtc.turn_servers` static credential entries now translate into
advertised ICE server entries, preserving `udp`, `tcp`, and `tls` protocol URL
shapes plus username and credential. A Rust SDK process contract starts the
binary from YAML and verifies the exact external TURN ICE entry in the join
response. Dynamic TURN credentials (`secret`, `secret_file`, `ttl`) remain
fail-closed because they require participant-specific HMAC credential
issuance; owned TURN/TLS remains unsupported.

### Slice E — multi-node relay parity blockers from gaps register ✅

**Completed:** relay join intents carry encoded client metadata to the selected
owner. The owner resolves ICE servers and client configuration through the same
`SignalState` queries as a local join, including participant-SID-specific TURN
credentials and browser codec policy. Malformed relay client metadata is
rejected rather than silently losing policy state.

`redis_relay_process_returns_room_owner_ice_servers` starts two real native
server processes against Redis, joins through the non-owning origin, and proves
that the SDK receives the selected owner’s ICE entry. The same process contract
uses RoomService list/get through the origin to prove management-plane forwarding
to the owner.

### Slice F — room policy/limit functionality

**Work:**

Implement deployment-relevant room defaults and limits that operators rely on
for safety (participants/metadata/identity/name and lifecycle policies).

**Done when:** unsupported room-policy fields move to translated with runtime
contracts.

## Open questions

1. Is transparent `--livekit-config` startup actually a product requirement, or
   is a checked migration tool sufficient and safer?
2. Which former deployment is the first concrete compatibility target? Its
   actual Compose file and `livekit.yaml` should become redacted fixtures.
3. Is a single API key sufficient for the target installation, or must
   multi-key rotation be implemented before migration?
4. Does the target require TCP/TLS TURN? If yes, choose external TURN first or
   fund a dedicated owned TURN/TLS implementation.
5. Is multi-node placement a requirement? If yes, close the relay ICE/client
   configuration gaps in [`../gaps.md`](../gaps.md) before treating the
   deployment as production-ready.

## Active handoff: Slice A external-IP compatibility

**Status (2026-07-17):** the candidate-mapping pipeline is committed and its
nested dependencies are published. Slice A is complete for the current
single-bind listener topology. Do not claim multi-interface or public-network
parity yet.

### Completed implementation and published revisions

The completed Slice A changes are recorded in OxideSFU commits
`248d3dd3` and `f8ad87db`:

- `ServerConfig` no longer rejects `rtc_use_external_ip=true` solely because
  `rtc_node_ip` is absent.
- `crates/oxidesfu-server/src/config.rs` has startup STUN discovery that:
  - uses the first configured `stun:` ICE URL, or
    `stun.l.google.com:19302` as fallback;
  - sends a STUN Binding request and reads `XOR-MAPPED-ADDRESS`;
  - retries three times with a 500 ms delay between attempts;
  - returns an error when discovery fails.
- `crates/oxidesfu-server/src/main.rs` resolves and assigns the discovered
  address before RTC/room-node/TURN startup.
- Core configuration tests were changed to accept external-IP mode with no
  explicit node IP.

Focused validation that passed after these edits:

```sh
cargo test -p oxidesfu-core --lib
cargo check -p oxidesfu-server
cargo run -p oxidesfu-server -- \
  config check-livekit ../othercode/livekit/config-sample.yaml
```

The upstream sample now passes the **configuration checker**, but that does not
prove usable public ICE candidates.

### Resolved WebRTC mapping finding

OxideSFU now pins the published outer WebRTC fork:

```text
andreeco/webrtc @ 6180766670e5a72a37a5b57ece067e021d69f443
```

Its nested RTC dependency is `andreeco/rtc @ 4018164`, both published on
`oxidesfu/external-ip-candidate-mappings`.

The prior public-IP-only `set_nat_1to1_ips(Vec<String>, CandidateType)` API
was stored but never reached the ICE agent and could not represent an interface
mapping. The published replacement is `Nat1To1IpMapping { local_ip,
external_ip }`:

1. `rtc-ice::AgentConfig` owns typed mappings.
2. outer `SettingEngine::set_nat_1to1_ip_mappings` forwards them through
   `RTCPeerConnection::new`.
3. matching host candidates replace only their advertised address; their
   `resolved_addr` remains the local socket address used for ICE I/O.
4. Oxide derives mappings from each configured UDP bind address and its
   configured/discovered external address.

Nested ICE, outer peer-connection event, and Oxide emitted-candidate tests
prove that mapped host candidates serialize the external address and do not
leak the wildcard/private advertised address.

### Upstream behavior reference

LiveKit source reference remains
`ae09b7d0ad94d764f0c97d183efd36476163e819`.

Relevant source map from that revision and its pinned
`mediatransportutil` dependency:

- `livekit/pkg/config/config.go` — embeds RTC config and validates it before
  server startup.
- `livekit/pkg/rtc/config.go` and `livekit/pkg/service/roommanager.go` — build
  WebRTC configuration during server initialization.
- pinned `mediatransportutil/pkg/rtcconfig/{config.go,ip.go,webrtc_config.go}`:
  - `use_external_ip=true` performs synchronous initial STUN discovery;
  - configured STUN servers are used, otherwise Twilio + Google defaults;
  - initial discovery retries three times with 500 ms waits and startup fails
    after exhaustion;
  - a later source-bound per-local-interface discovery pass produces
    external/local candidate rewrite mappings;
  - unresolved mapping discovery degrades to a discovered/configured node-IP
    fallback, while invalid setup/bind conditions remain fatal.

### Remaining external-IP work

Do **not** merely remove validation or advertise one discovered address
blindly. Implement this sequence:

1. Enumerate each concrete local interface when the RTC listener uses a
   wildcard bind, then perform source-bound STUN discovery and pass one mapping
   per interface. The current implementation source-binds discovery for a
   concrete configured RTC bind and maps the current wildcard listener shape,
   but it does not enumerate interfaces.
2. Add deterministic STUN retry/timeout and explicit-node-IP regression tests.
3. Add a real external deployment contract from a separate host, VM, or network
   namespace proving public candidate connectivity.
4. Add server-reflexive mapping mode only when a compatibility contract requires
   it; current LiveKit-compatible host replacement is deliberately explicit.

### Handoff cautions

- The local `webrtc` checkout is reference/development material. OxideSFU must
  not add a local path dependency.
- Do not commit unrelated existing OxideSFU worktree changes.
- The existing personal note directory is ignored intentionally; this note is
  local operator/agent memory, not a substitute for focused source commits.
- Per-interface discovery, candidate rewrite behavior, and public-network
  validation are separate from merely making `config check-livekit` succeed.

## Conclusion

LiveKit compatibility should mean more than a process listening on a familiar
port. For operators, deployment configuration is part of the observable server
contract.

The pragmatic path is not to promise full YAML parity now and not to replace
OxideSFU's native configuration model. Build a strict LiveKit-YAML migration
adapter first, support a deliberately small single-node subset with end-to-end
proof, then optionally add YAML startup as a thin adapter over the same
normalized configuration and validation path. Expand accepted YAML only when
the corresponding runtime behaviour has tests.
