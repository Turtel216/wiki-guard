#!/usr/bin/env bash
# Bring up the local dev cluster: k3s inside Docker, the Flux controllers,
# then the dev overlay, our own images from the working tree, and the
# dashboards. Safe to run again.
set -euo pipefail

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
. "$(dirname "${BASH_SOURCE[0]}")/k3d.sh"
need docker k3d kubectl make

CLUSTER="wikiguard-dev"
CONTEXT="k3d-$CLUSTER"
OVERLAY="$REPO_ROOT/deploy/overlays/dev"

start_cluster --k3s-arg "--disable=traefik@server:0"
install_flux
install_operators
install_platform
install_ingestor dev
install_monitoring

echo
echo "The dev cluster is up. Look around with:"
echo "  kubectl --context $CONTEXT -n platform get pods"
echo "  kubectl --context $CONTEXT -n wikiguard logs deployment/ingestor"
echo "The dashboards are at http://localhost:3000 while this runs:"
echo "  kubectl --context $CONTEXT -n monitoring port-forward service/grafana 3000:3000"
