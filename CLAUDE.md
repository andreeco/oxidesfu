# CLAUDE.md

This file defines how AI coding agents must work on **OxideSFU**, the project stored in the local workspace directory `oxidesfu/`.

OxideSFU aims to be a test-driven Rust implementation of a WebRTC SFU/server with **LiveKit-compatible wire protocols**. The goal is to let existing LiveKit clients, SDKs, CLI commands, and Twirp API clients connect unchanged.

## Mission

Build a Rust WebRTC server compatible with the existing LiveKit ecosystem.

The implementation must be developed with strict TDD. No significant behavior should be implemented without first adding or updating tests that describe the required compatibility or internal behavior.

## Reference repositories in this workspace

Primary implementation repo:

- `oxidesfu/` — target implementation repository for OxideSFU.

Reference/compatibility repos:

- `webrtc/` — local `webrtc-rs` source inspection checkout (reference only).
- `sfu/` — existing Rust SFU prototype/reference.
- `livekit/` — official Go LiveKit server; executable behavior spec.
- `livekit-cli/` — CLI compatibility target.
- `server-sdk-go/` — Go server SDK compatibility target.
- `rust-sdks/` — Rust protocol/API compatibility target.
- `client-sdk-js/` — JS client compatibility target.


Before relying on behavior from any moving repository, verify current commits and record what you used in session notes or a design note.

## Non-negotiable engineering rules

1. **TDD first**
   - Add or update tests before implementing behavior.
   - For compatibility features, prefer black-box tests against real clients (`rust-sdks`, `livekit-cli`, `server-sdk-go`, client SDKs) when practical.
   - A feature is not done until tests pass and compatibility expectations are documented.

2. **Compatibility first**
   - Existing SDKs and `livekit-cli` should not require changes.
   - Preserve required wire-level names and protocol shapes where compatibility requires it.
   - If exact compatibility is deferred, document the gap in design notes and in tests (`ignored`/`todo`) only when necessary.

3. **Use source code as spec, not memory**
   - Do not guess protocol behavior.
   - Map relevant reference files before coding each feature.
   - Summarize inspected files, learned behavior, derived tests, and unresolved questions in a design note.

4. **Use `axum`**
   - HTTP/WebSocket server implementation should use `axum` unless there is a documented reason not to.
   - Use `tower` middleware idiomatically for auth, tracing, limits, body handling, and request context.

5. **Use latest Rust edition**
   - New crates should use the latest stable Rust edition available to the toolchain.

6. **Use current crates.io dependencies**
   - Before adding/changing a crates.io dependency, check latest stable release (`cargo search` or `cargo info`).
   - If a dependency must stay behind latest for compatibility, document the reason in `Cargo.toml` and in design notes.
   - Keep `Cargo.lock` committed for reproducible builds.

7. **Use pinned `webrtc-rs` from GitHub**
   - Depend on `webrtc-rs` via GitHub pinned commit, not local path dependency.
   - Use local `webrtc/` checkout for inspection only.
   - Record the exact commit used when integrating/changing behavior.

8. **Minimal public API**
   - Default to private or `pub(crate)` APIs.
   - Add public APIs only with clear need and concise docs.

9. **No mechanical Go porting**
   - Use Go server as behavior reference.
   - Port behavior, not file structure.
   - Use Rust-native architecture, errors, ownership, and async patterns.

10. **Safe Rust**
    - Avoid `unsafe` unless absolutely necessary.
    - Avoid `unwrap` outside tests.

11. **Format, lint, and validate before commit**
    - Run `cargo fmt --all`.
    - Run targeted tests for changed code.
    - Run `cargo test --workspace` before commit (unless explicitly told otherwise).
    - Run `cargo clippy --workspace --all-targets -- -D warnings` before calling a major compatibility slice done.
    - If running conformance suites under `tools/conformance/`, local workspace tests should also pass unless explicitly waived for a specific investigation.
    - Commit meaningful, validated slices regularly. Do not commit unrelated work already present in the working tree.

## Mandatory git history and documentation workflow

Git history is the durable progress record.

At the beginning of a session, inspect the full messages for the latest 20–30 commits (if it makes sense):

```bash
git --no-pager log -n 30
```

Use that history to avoid duplicating work, identify active compatibility investigations, and preserve relevant conventions.

For every meaningful completed change or investigation, create a focused git commit. Its subject and body must make the work understandable from `git log` / `git show`, including as applicable:

- objective and behavior changed or investigated,
- relevant reference repositories/files and commit IDs,
- tests added or changed,
- validation commands and pass/fail outcomes,
- known compatibility gap and the next concrete step.

Keep commit subjects imperative and concise; use the commit body for the evidence above. Do not leave project memory only in chat.

A Markdown design note is still appropriate only when a durable design, protocol mapping, decision, or unresolved compatibility gap needs more detail than a commit message can reasonably carry. It must be committed with the related code or investigation; it is not a replacement for a useful commit message.

## Mandatory investigation workflow before coding

Before implementing a feature, map relevant reference files first. Do not guess LiveKit-compatible behavior.

For each feature, record a short **Reference map** in the related commit body, test documentation, or—only when warranted—a focused design note:

- files/directories inspected,
- behavior learned,
- tests derived from that behavior,
- unresolved questions.

### Reference map by repository area (keep this updated as code moves)

Use these as starting trees rather than frozen file checklists:

- Rust SDK protocol/signalling: `rust-sdks/livekit-protocol/src/`, `rust-sdks/livekit-api/src/`
- Go server signalling/room/Twirp: `livekit/pkg/service/`, `livekit/pkg/rtc/`
- Go SFU/media behavior: `livekit/pkg/sfu/`
- Rust SFU design inspiration: `sfu/src/`, `sfu/examples/`
- `webrtc-rs` API shape: `webrtc/src/`, `webrtc/examples/`, `webrtc/README.md`
- CLI behavior: `livekit-cli/cmd/lk/`
- Go server SDK expectations: `server-sdk-go/` (especially integration tests and room/service clients)
- JS client expectations: `client-sdk-js/` (join/publish/subscribe/signaling paths)


If you rely on specific files, record their exact paths and relevant reference commit IDs in the related commit body or focused design note.

## Compatibility policy

Compatibility is **proven by tests**, not by maintaining a static endpoint checklist in this file.

- Keep compatibility contract tests current.
- Prefer SDK/CLI black-box and conformance checks over assumptions.
- When behavior differs from upstream, document the delta and test coverage explicitly.

## Useful commands

Core validation commands:

```bash
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo run -p oxidesfu-server -- --dev --bind 127.0.0.1:7880 --api-key devkey --api-secret secret
```

`lk` usage rule for local testing:

- Always pass `--url` explicitly so commands target the local OxideSFU server.
- Do not rely on `lk` defaults or environment variables that may point to production LiveKit.

CLI compatibility commands (local OxideSFU):

```bash
# Localhost example
lk --url http://127.0.0.1:7880 --api-key devkey --api-secret secret room create test-room
lk --url http://127.0.0.1:7880 --api-key devkey --api-secret secret room list
lk --url http://127.0.0.1:7880 --api-key devkey --api-secret secret room delete test-room
lk --url ws://127.0.0.1:7880 --api-key devkey --api-secret secret room join --identity alice test-room

# LAN example (replace with your machine's local IP)
lk --url http://192.168.1.50:7880 --api-key devkey --api-secret secret room list
```

Token mint example (aligned with `deploy/README.md` style):

```bash
lk token create \
  --api-key devkey \
  --api-secret secret \
  --identity admin-user \
  --room test-room \
  --join \
  --admin \
  --valid-for 24h \
  --token-only
```

When testing from another device on your LAN, replace `127.0.0.1` with the machine local IP (for example `192.168.1.50`) in server bind/URLs as appropriate.
