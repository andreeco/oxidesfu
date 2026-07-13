#!/usr/bin/env bash
set -euo pipefail

if command -v rustup >/dev/null 2>&1; then
  rustup component add rustfmt clippy
fi

if command -v corepack >/dev/null 2>&1; then
  corepack enable
  corepack prepare pnpm@latest --activate
fi

echo "Devcontainer post-create complete."
echo "Workspace root: $(pwd)"
