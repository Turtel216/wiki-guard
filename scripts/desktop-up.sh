#!/usr/bin/env bash
# Bring up the desktop cluster: the setup of the home server, run in k3d on
# the desktop while the server is out of service. Unlike the dev cluster,
# its ingestor connects to the live Wikimedia stream.
#
# Safe to run again. That rebuilds the ingestor from the working tree and
# restarts it, and it resumes right after the last event it stored. After a
# reboot nothing needs to be run: Docker starts the cluster again, if the
# Docker service itself starts at boot.
set -euo pipefail

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
. "$(dirname "${BASH_SOURCE[0]}")/k3d.sh"
need docker k3d kubectl make openssl git

CLUSTER="wikiguard-desktop"
CONTEXT="k3d-$CLUSTER"
OVERLAY="$REPO_ROOT/deploy/overlays/desktop"
GRAFANA_PORT="${GRAFANA_PORT:-3000}"

# Random credentials, made once and kept only in the cluster. PostgreSQL
# keeps the password it was first started with, so an existing secret is
# never replaced.
create_secret() {
  local name="$1"
  shift
  if kc -n platform get secret "$name" >/dev/null 2>&1; then
    echo "secret $name already exists"
  else
    kc -n platform create secret generic "$name" "$@"
  fi
}

# Both clusters at once leave little memory for anything else.
if docker ps --format '{{.Names}}' | grep -qx "k3d-wikiguard-dev-server-0"; then
  echo "warning: the dev cluster is running too, stop it with: k3d cluster stop wikiguard-dev" >&2
fi

# The image is built from the working tree, so say when that is not the commit.
if [ -n "$(git -C "$REPO_ROOT" status --porcelain -- jvm config proto)" ]; then
  echo "warning: jvm/, config/ or proto/ has uncommitted changes, they go into the image" >&2
fi

# Grafana is served through the ingress, on this machine only.
start_cluster --port "127.0.0.1:$GRAFANA_PORT:80@loadbalancer"
install_flux
install_operators

echo "== creating credentials"
create_secret postgres-credentials --from-literal=password="$(openssl rand -hex 24)"
create_secret seaweedfs-s3 \
  --from-literal=access_key=wikiguard \
  --from-literal=secret_key="$(openssl rand -hex 24)"

install_platform
install_ingestor desktop
# Records which commit the running ingestor was built from.
kc -n wikiguard annotate deployment/ingestor --overwrite \
  "wikiguard.dev/commit=$(git -C "$REPO_ROOT" rev-parse --short HEAD)"
install_monitoring

echo
echo "The desktop cluster is up and the ingestor reads the live stream. Check it with:"
echo "  kubectl --context $CONTEXT -n wikiguard logs -f deployment/ingestor"
echo "The dashboards are at http://localhost:$GRAFANA_PORT"
