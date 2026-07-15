#!/usr/bin/env bash
set -euo pipefail

if command -v rustup >/dev/null 2>&1; then
  rustup component add rustfmt clippy
fi

if command -v corepack >/dev/null 2>&1; then
  corepack enable
  corepack prepare pnpm@latest --activate
fi

if command -v cargo >/dev/null 2>&1; then
  command -v flamegraph >/dev/null 2>&1 || cargo install --locked flamegraph
  command -v tokio-console >/dev/null 2>&1 || cargo install --locked tokio-console
fi

echo "Devcontainer post-create complete."
echo "Workspace root: $(pwd)"
