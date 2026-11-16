#!/usr/bin/env bash
# Bring up the local dev cluster: k3s inside Docker, the Flux controllers,
# then the dev overlay, our own images from the working tree, and the
# dashboards. Safe to run again.
#
# Every kubectl call names the dev context, so this can never touch the
# home server even when that is the current context.
set -euo pipefail

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need docker k3d kubectl make

CLUSTER="wikiguard-dev"
CONTEXT="k3d-$CLUSTER"
OVERLAY="$REPO_ROOT/deploy/overlays/dev"

# Image tags cannot contain a plus sign, so k3s publishes v1.2.3+k3s1 as v1.2.3-k3s1.
K3S_IMAGE="rancher/k3s:$(pinned platform.k3s | tr '+' '-')"
INGESTOR_IMAGE="ghcr.io/turtel216/wikiguard-ingestor:dev"
FLUX_MANIFEST="https://github.com/fluxcd/flux2/releases/download/$(pinned platform.flux)/install.yaml"

kc() {
  kubectl --context "$CONTEXT" "$@"
}

if k3d cluster list "$CLUSTER" >/dev/null 2>&1; then
  echo "== cluster $CLUSTER already exists, starting it"
  k3d cluster start "$CLUSTER"
else
  echo "== creating cluster $CLUSTER"
  k3d cluster create "$CLUSTER" \
    --image "$K3S_IMAGE" \
    --k3s-arg "--disable=traefik@server:0" \
    --kubeconfig-switch-context=false \
    --wait
fi

echo "== installing the Flux controllers"
kc apply --server-side -f "$FLUX_MANIFEST"
kc -n flux-system wait deployment --all --for=condition=Available --timeout=5m

echo "== installing operators"
kc apply -k "$OVERLAY/operators"
kc -n platform wait helmrelease/strimzi-kafka-operator --for=condition=Ready --timeout=10m

echo "== installing Kafka, SeaweedFS and PostgreSQL"
kc apply -k "$OVERLAY/platform"
kc -n platform rollout status statefulset/postgres --timeout=5m
kc -n platform rollout status statefulset/seaweedfs --timeout=5m
kc -n platform wait job/seaweedfs-buckets --for=condition=Complete --timeout=5m
kc -n platform wait kafka/wikiguard --for=condition=Ready --timeout=15m
kc -n platform wait kafkatopic --all --for=condition=Ready --timeout=5m

echo "== building and installing the WikiGuard services"
make -C "$REPO_ROOT" image-ingestor
k3d image import --cluster "$CLUSTER" "$INGESTOR_IMAGE"
kc apply -k "$OVERLAY/wikiguard"
# The image tag never changes in dev, so a restart is what picks up a new build.
kc -n wikiguard rollout restart deployment/ingestor
kc -n wikiguard rollout status deployment/ingestor --timeout=5m

echo "== installing Prometheus and Grafana"
kc apply -k "$OVERLAY/monitoring"
kc -n monitoring rollout status statefulset/prometheus --timeout=5m
kc -n monitoring rollout status deployment/grafana --timeout=5m

echo
echo "The dev cluster is up. Look around with:"
echo "  kubectl --context $CONTEXT -n platform get pods"
echo "  kubectl --context $CONTEXT -n wikiguard logs deployment/ingestor"
echo "The dashboards are at http://localhost:3000 while this runs:"
echo "  kubectl --context $CONTEXT -n monitoring port-forward service/grafana 3000:3000"
