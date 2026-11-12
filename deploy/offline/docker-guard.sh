#!/usr/bin/env bash
set -euo pipefail
case "$*" in
 version*|"image inspect "*) exec /usr/local/bin/docker-real "$@" ;;
 *) echo 'offline runner refuses Docker builds, pulls and mutations' >&2; exit 1 ;;
esac
