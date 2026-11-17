# Steps shared by the scripts that bring up a local k3d cluster from the
# overlays under deploy/. Source it after lib.sh, then set CLUSTER, CONTEXT
# and OVERLAY before calling anything here.
#
# Every kubectl call names the context of that cluster, so these steps can
# never touch another cluster, even when it is the current context.

kc() {
  kubectl --context "$CONTEXT" "$@"
}

# Create the cluster, or start it if it exists already. The arguments are
# passed on to "k3d cluster create", so they only count the first time.
start_cluster() {
  local image
  # Image tags cannot contain a plus sign, so k3s publishes v1.2.3+k3s1 as v1.2.3-k3s1.
  image="rancher/k3s:$(pinned platform.k3s | tr '+' '-')"
  if k3d cluster list "$CLUSTER" >/dev/null 2>&1; then
    echo "== cluster $CLUSTER already exists, starting it"
    k3d cluster start "$CLUSTER"
  else
    echo "== creating cluster $CLUSTER"
    k3d cluster create "$CLUSTER" \
      --image "$image" \
      --kubeconfig-switch-context=false \
      --wait \
      "$@"
  fi
}

install_flux() {
  local manifest
  manifest="https://github.com/fluxcd/flux2/releases/download/$(pinned platform.flux)/install.yaml"
  echo "== installing the Flux controllers"
  kc apply --server-side -f "$manifest"
  kc -n flux-system wait deployment --all --for=condition=Available --timeout=5m
}

install_operators() {
  echo "== installing operators"
  kc apply -k "$OVERLAY/operators"
  kc -n platform wait helmrelease/strimzi-kafka-operator --for=condition=Ready --timeout=10m
}

install_platform() {
  echo "== installing Kafka, SeaweedFS and PostgreSQL"
  kc apply -k "$OVERLAY/platform"
  kc -n platform rollout status statefulset/postgres --timeout=5m
  kc -n platform rollout status statefulset/seaweedfs --timeout=5m
  kc -n platform wait job/seaweedfs-buckets --for=condition=Complete --timeout=5m
  kc -n platform wait kafka/wikiguard --for=condition=Ready --timeout=15m
  kc -n platform wait kafkatopic --all --for=condition=Ready --timeout=5m
}

# Build the ingestor image from the working tree under the given tag, load
# it into the cluster and roll it out. The overlay must name the same tag.
install_ingestor() {
  local tag="$1"
  echo "== building and installing the WikiGuard services"
  make -C "$REPO_ROOT" image-ingestor IMAGE_TAG="$tag"
  k3d image import --cluster "$CLUSTER" "ghcr.io/turtel216/wikiguard-ingestor:$tag"
  kc apply -k "$OVERLAY/wikiguard"
  # The tag never changes between builds, so a restart is what picks up a new one.
  kc -n wikiguard rollout restart deployment/ingestor
  kc -n wikiguard rollout status deployment/ingestor --timeout=5m
}

install_monitoring() {
  echo "== installing Prometheus and Grafana"
  kc apply -k "$OVERLAY/monitoring"
  kc -n monitoring rollout status statefulset/prometheus --timeout=5m
  kc -n monitoring rollout status deployment/grafana --timeout=5m
}
