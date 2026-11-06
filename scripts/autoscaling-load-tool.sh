#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
version="${K6_VERSION:-$(make -s print-K6_VERSION)}"
version="${version#v}"
cache="${XDG_CACHE_HOME:-$HOME/.cache}/reliefgrid-autoscaling"
mkdir -p "$cache"
archive="$cache/k6-v$version-linux-amd64.tar.gz"
if [ ! -f "$archive" ]; then
  curl --fail --location --silent --show-error --connect-timeout 10 --max-time 120 \
    "https://github.com/grafana/k6/releases/download/v$version/k6-v$version-linux-amd64.tar.gz" -o "$archive"
fi
printf '%s  %s\n' "$(make -s print-K6_SHA256)" "$archive" | sha256sum -c -
tar -xzf "$archive" -C "$cache" "k6-v$version-linux-amd64/k6"
"$cache/k6-v$version-linux-amd64/k6" version
