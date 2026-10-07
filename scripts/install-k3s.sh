#!/usr/bin/env bash
# Install the pinned k3s release as a single-node server. Run it on the
# home server, as root, from a checkout of this repository. Running it
# again with a newer pinned version upgrades k3s in place.
set -euo pipefail

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need curl

if [ "$(id -u)" -ne 0 ]; then
  echo "run this as root, for example with sudo" >&2
  exit 1
fi

K3S_VERSION="$(pinned platform.k3s)"
echo "== installing k3s $K3S_VERSION"

# The label marks this node as the one that runs everything always-on.
curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION="$K3S_VERSION" sh -s - server \
  --node-label wikiguard.dev/tier=core

echo "== waiting for the node to become ready"
k3s kubectl wait node --all --for=condition=Ready --timeout=3m
k3s kubectl get nodes -o wide
