# OxideSFU devcontainer

This devcontainer is tuned for running OxideSFU plus the conformance scripts in `tools/conformance/`.

## Why `workspaceMount` points to parent directory

Conformance scripts expect sibling repos relative to `oxidesfu/`, for example:

- `../othercode/livekit`
- `../othercode/livekit-cli`
- `../othercode/server-sdk-go`
- `../othercode/rust-sdks`
- `../othercode/client-sdk-js`
- `../webrtc`

`devcontainer.json` mounts `${localWorkspaceFolder}/..` into `/workspace`, and sets
`workspaceFolder` to `/workspace/oxidesfu`, so those relative paths continue to work.

## Host checkout layout required

Before opening the container, clone these repos on the host in the same parent directory as `oxidesfu/`.

Expected host layout example:

- `rustprojects/oxidesfu`
- `rustprojects/webrtc`
- `rustprojects/othercode/livekit`
- `rustprojects/othercode/livekit-cli`
- `rustprojects/othercode/server-sdk-go`
- `rustprojects/othercode/rust-sdks`
- `rustprojects/othercode/client-sdk-js`

## Clone your fork branches (recommended)

From the parent directory (`rustprojects/` in this example):

```bash
# OxideSFU implementation
# (assumes this repo already exists locally)

# webrtc fork + branch used for source inspection and compatibility work
# target path must be ../webrtc relative to oxidesfu
cd ~/rustprojects
git clone git@github.com:andreeco/webrtc.git webrtc
cd webrtc
git checkout oxidesfu/webrtc-compat

# livekit fork + branch used by conformance script defaults
mkdir -p ~/rustprojects/othercode
cd ~/rustprojects/othercode
git clone git@github.com:andreeco/livekit.git livekit
cd livekit
git checkout oxidesfu/livekit-compat
```

If a repository already exists locally, just add/update the fork remote and checkout the branch.

## Required repos and where to clone from

Use this source map for sibling repositories required by `tools/conformance/*`:

| Local path | Repository URL | Branch recommendation |
|---|---|---|
| `../webrtc` | `git@github.com:andreeco/webrtc.git` | `oxidesfu/webrtc-compat` |
| `../othercode/livekit` | `git@github.com:andreeco/livekit.git` | `oxidesfu/livekit-compat` |
| `../othercode/livekit-cli` | `https://github.com/livekit/livekit-cli.git` | default branch |
| `../othercode/server-sdk-go` | `https://github.com/livekit/server-sdk-go.git` | default branch |
| `../othercode/rust-sdks` | `https://github.com/livekit/rust-sdks.git` | default branch |
| `../othercode/client-sdk-js` | `https://github.com/livekit/client-sdk-js.git` | default branch |

## Quick verification inside container

```bash
rustc --version
cargo --version
go version
node --version
pnpm --version
protoc --version
turnserver --version
```

Rust in this devcontainer is configured as `stable` (latest stable release).

## Example conformance runs inside container

```bash
tools/conformance/server-sdk-go-full-suite.sh
tools/conformance/livekit-full-suite-all.sh
tools/conformance/livekit-cli-full-suite.sh
tools/conformance/rust-sdks-full-suite.sh
tools/conformance/client-sdk-js-full-suite.sh
```

If any repo is in a non-default location, set its corresponding environment variable (`LIVEKIT_REPO`, `LIVEKIT_CLI_REPO`, `SERVER_SDK_GO`, `RUST_SDKS_REPO`, `CLIENT_SDK_JS_REPO`) before running a script.
