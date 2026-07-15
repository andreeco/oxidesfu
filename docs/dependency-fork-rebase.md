# Rebasing dependency forks safely

OxideSFU uses a small number of Git dependency forks for compatibility work. Each
consumer pins an immutable commit in `Cargo.toml` and `Cargo.lock`; never depend
on a moving branch for a reproducible build.

This guide uses **upstream** for the original project remote and **fork** for the
`andreeco` remote. Confirm the actual remote names with `git remote -v` before
running any command. For example, the local `rust-sdks` checkout currently uses
`origin` for upstream LiveKit and `andreeco` for the fork.

## Required sequence

1. Start clean and inspect the published compatibility branch.
2. Fetch both remotes.
3. Create and push a date-stamped backup **tag** and backup **branch** from the
   current fork branch.
4. Create a date-stamped rebase work branch. Do not rebase the maintained
   compatibility branch in place.
5. Rebase the work branch onto the intended upstream commit or branch.
6. Resolve each conflict from upstream behavior and OxideSFU compatibility
   evidence; never use blanket `--ours` or `--theirs` resolution.
7. Run the dependency's tests, then downstream OxideSFU tests.
8. Push the work branch for review. Only after validation, update the maintained
   fork branch using `--force-with-lease`.
9. Pin the resulting immutable commit in OxideSFU, update `Cargo.lock`, validate
   again, and commit the pin update separately.

A backup tag is the durable recovery point; the backup branch is convenient for
inspection and restoration. Use a descriptive ISO-date name, for example:

```text
backup-tag/rust-sdks-compat-2026-07-15-pre-upstream-rebase
backup/rust-sdks-compat-2026-07-15-pre-upstream-rebase
rebase/rust-sdks-compat-2026-07-15-upstream-main
```

Never use plain `git push --force`. `--force-with-lease` refuses to overwrite a
remote branch that changed since it was fetched.

## Generic commands

Replace the placeholders with the repository's actual branch names and remotes.

```sh
# Refuse to proceed with uncommitted work.
git status --short

git fetch upstream
git fetch fork

git switch <compatibility-branch>
git reset --hard fork/<compatibility-branch>

# Preserve the exact published state before rewriting history.
git tag backup-tag/<fork>-<date>-pre-upstream-rebase
git push fork refs/tags/backup-tag/<fork>-<date>-pre-upstream-rebase

git branch backup/<fork>-<date>-pre-upstream-rebase
git push fork refs/heads/backup/<fork>-<date>-pre-upstream-rebase

# Perform the risky operation away from the maintained branch.
git switch -c rebase/<fork>-<date>-upstream-main
git rebase --rebase-merges upstream/<upstream-branch>

# Inspect the resulting patch and compatibility commits.
git --no-pager diff upstream/<upstream-branch>...HEAD
git --no-pager log --oneline upstream/<upstream-branch>..HEAD

# Push a reviewable candidate first.
git push -u fork rebase/<fork>-<date>-upstream-main

# After validation only.
git push --force-with-lease fork HEAD:<compatibility-branch>
```

If the fork history is intentionally linear, `git rebase upstream/<upstream-branch>`
is acceptable instead of `--rebase-merges`.

## Rust SDK fork

The Rust SDK is a public SDK surface. Prefer a dedicated maintained branch such
as `oxidesfu/compat` rather than continuously rewriting the fork's `main` branch.
Record the exact retained compatibility commits after every rebase; the count may change when upstream incorporates or supersedes a patch.

After rebasing, validate in `rust-sdks`:

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Follow the Rust SDK's contribution requirements as well: preserve public API
compatibility, avoid unnecessary dependencies, and create a `knope` changeset if
the rebased fork changes publishable SDK crates.

Then update all three Rust SDK test dependencies in
`crates/oxidesfu-test/Cargo.toml` to the same new immutable revision:

| Dependency | Purpose |
| --- | --- |
| `livekit` | Rust client SDK compatibility probes |
| `livekit-api` | access token and service-client probes |
| `livekit-protocol` | protocol types used by the probes |

Refresh the lockfile and validate OxideSFU:

```sh
# Updating `livekit` refreshes all packages from this Git source.
# Do not run `cargo update -p livekit-protocol`: OxideSFU also uses a
# crates.io package with that name, so Cargo correctly treats it as ambiguous.
cargo update -p livekit
cargo update -p livekit-api
cargo test -p oxidesfu-test
cargo test --workspace
```

Run relevant Rust SDK conformance commands in `tools/conformance/` as well.

## WebRTC and RTC forks

The outer `webrtc` fork contains the RTC core as a submodule. Rebase and publish
in this order:

1. Rebase and validate the RTC core fork.
2. Update the outer `webrtc` fork submodule pointer to the final RTC commit.
3. Rebase and validate the outer `webrtc` fork.
4. Pin the final outer commit in OxideSFU.

The outer fork must pass its focused remote-track and forwarding tests before the
OxideSFU pin moves. The current OxideSFU workspace dependencies are:

| Dependency | Pin location | Rebase/pin rule |
| --- | --- | --- |
| `webrtc` | root `Cargo.toml` workspace dependencies | Pin the final outer fork commit. |
| `rtc` | root `Cargo.toml` workspace dependencies | Keep at the same outer fork commit as `webrtc`. |
| `rtc-stun` | root `Cargo.toml` workspace dependencies | Separate historical fork pin; update only when its own compatibility change is rebased and validated. |

After updating `webrtc` or `rtc`, run `cargo update -p webrtc` and commit the
resulting `Cargo.toml` and `Cargo.lock` change. At minimum run the focused RTC
and signaling tests plus the Firefox browser receiver contract.

## Other current Git forks

| Dependency | Pin location | Notes |
| --- | --- | --- |
| `turn-server` | root `Cargo.toml` workspace dependencies | Rebase its `andreeco/turn-rs` compatibility branch independently; validate TURN allocation/auth and OxideSFU TURN integration. |
| Rust SDK crates | `crates/oxidesfu-test/Cargo.toml` dev-dependencies | Always move `livekit`, `livekit-api`, and `livekit-protocol` together to one Rust SDK revision. |

Do not change crates.io dependencies such as the root workspace's production
`livekit-protocol = "0.7.10"` merely because the Rust SDK test fork advances.
They are separate dependency roles and must be upgraded deliberately.

## Commit record

Every completed rebase slice should record:

- upstream repository, branch, and exact base commit;
- backup tag and backup branch;
- resulting fork commit and compatibility branch;
- conflicts resolved, including why each retained patch remains needed;
- dependency and downstream validation commands with their outcomes;
- all changed OxideSFU immutable pins and lockfile updates.
