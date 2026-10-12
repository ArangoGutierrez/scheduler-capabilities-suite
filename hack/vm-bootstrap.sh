#!/bin/bash
# Prepare a Linux VM (amd64 or arm64) for a full suite run: install Go
# 1.27.2, verified by sha256, under $PREFIX (default $HOME/.local), and check
# Docker access, the tools the Makefile uses, and outbound access to every
# host the suite pulls from.
#   bash hack/vm-bootstrap.sh
set -euo pipefail
GO_VERSION=1.27.2
PREFIX=${PREFIX:-$HOME/.local}
case "$PREFIX" in
  "" | "/") echo "vm-bootstrap: refusing PREFIX='$PREFIX'" >&2; exit 2 ;;
esac
[ "$(uname -s)" = Linux ] || { echo "vm-bootstrap: Linux only" >&2; exit 2; }
case "$(uname -m)" in
  x86_64) arch=amd64 sum=ecbadb99091a3f46e31f5f934b068b1864eafa7995211b39eaddf76996045fe5 ;;
  aarch64 | arm64) arch=arm64 sum=94f3e30b8e374bc285e7dadc11e0865726b9bc6e85b841ccceaabc0214c6b7c8 ;;
  *) echo "vm-bootstrap: unsupported architecture $(uname -m)" >&2; exit 2 ;;
esac
for tool in docker git make curl tar sha256sum; do
  command -v "$tool" >/dev/null || { echo "vm-bootstrap: $tool is missing" >&2; exit 2; }
done
docker info >/dev/null 2>&1 || {
  echo "vm-bootstrap: this user cannot reach the Docker daemon (add it to the docker group, then log in again)" >&2
  exit 2
}
tarball=go${GO_VERSION}.linux-${arch}.tar.gz
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl -sSfL -o "$tmp/$tarball" "https://go.dev/dl/$tarball"
echo "$sum  $tmp/$tarball" | sha256sum -c - >/dev/null || { echo "vm-bootstrap: $tarball checksum mismatch" >&2; exit 3; }
mkdir -p "$PREFIX"
rm -rf "$PREFIX/go"
tar -C "$PREFIX" -xzf "$tmp/$tarball"
"$PREFIX/go/bin/go" version
for host in registry-1.docker.io registry.k8s.io github.com raw.githubusercontent.com proxy.golang.org; do
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "https://$host/" || true)
  if [ -z "$code" ] || [ "$code" = 000 ]; then
    echo "vm-bootstrap: no route to $host" >&2
    exit 4
  fi
  echo "reachable: $host (HTTP $code)"
done
echo "docker: $(docker info --format '{{.ServerVersion}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes')"
echo "next: export PATH=$PREFIX/go/bin:\$PATH"
