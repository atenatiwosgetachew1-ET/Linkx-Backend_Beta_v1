#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 /path/to/linkx-artifacts.tar.gz /empty/restore/target" >&2
  exit 2
fi

archive="$1"
target_dir="$2"

if [ ! -r "${archive}" ]; then
  echo "artifact archive is not readable: ${archive}" >&2
  exit 2
fi

case "${target_dir}" in
  /|/mnt|/mnt/linkx-artifacts|/mnt/linkx-artifacts/)
    echo "refusing unsafe restore target: ${target_dir}" >&2
    exit 2
    ;;
esac

mkdir -p "${target_dir}"
if [ -n "$(find "${target_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
  echo "restore target must be empty: ${target_dir}" >&2
  exit 2
fi

if [ -f "${archive}.sha256" ]; then
  sha256sum -c "${archive}.sha256"
fi

tar --xattrs --acls -xzf "${archive}" -C "${target_dir}"
find "${target_dir}" -maxdepth 2 -type d | sort | head -40

echo "restored ${archive} into ${target_dir}"
