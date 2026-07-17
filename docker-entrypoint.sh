#!/bin/sh
set -eu

secret_dir=/run/oxidesfu-secrets
install -d -m 0700 -o oxidesfu -g oxidesfu "$secret_dir"

copy_secret_file() {
  source_path=$1
  destination_name=$2
  destination_path="$secret_dir/$destination_name"
  install -m 0400 -o oxidesfu -g oxidesfu "$source_path" "$destination_path"
  printf '%s' "$destination_path"
}

if [ -n "${OXIDESFU_API_KEY_FILE:-}" ]; then
  OXIDESFU_API_KEY_FILE="$(copy_secret_file "$OXIDESFU_API_KEY_FILE" api-key)"
  export OXIDESFU_API_KEY_FILE
fi

if [ -n "${OXIDESFU_API_SECRET_FILE:-}" ]; then
  OXIDESFU_API_SECRET_FILE="$(copy_secret_file "$OXIDESFU_API_SECRET_FILE" api-secret)"
  export OXIDESFU_API_SECRET_FILE
fi

exec setpriv --reuid=oxidesfu --regid=oxidesfu --init-groups "$@"
