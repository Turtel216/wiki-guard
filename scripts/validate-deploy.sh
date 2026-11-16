#!/usr/bin/env bash
# Check the manifests under deploy/ without a cluster: every overlay must
# build, every resource must match its schema, and versions written into
# manifests must match versions.toml.
set -euo pipefail

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need kubectl kubeconform

DEPLOY="$REPO_ROOT/deploy"
# Schemas for resource types that do not ship with Kubernetes itself.
CRD_SCHEMAS='https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'

conform() {
  kubeconform -strict -summary -ignore-missing-schemas \
    -schema-location default -schema-location "$CRD_SCHEMAS" "$@"
}

failed=0

for kustomization in "$DEPLOY"/overlays/*/*/kustomization.yaml; do
  dir="$(dirname "$kustomization")"
  echo "== ${dir#"$REPO_ROOT"/}"
  kubectl kustomize "$dir" | conform - || failed=1
done

echo "== deploy/clusters/home"
conform "$DEPLOY"/clusters/home/*.yaml || failed=1

# Manifests cannot read versions.toml, so make sure the copies agree with it.
check_pin() {
  local value file="$REPO_ROOT/$2"
  value="$(pinned "$1")"
  if ! grep -qF -- "$value" "$file"; then
    echo "$2 does not use $1 = $value from versions.toml" >&2
    failed=1
  fi
}

echo "== pinned versions"
check_pin platform.strimzi deploy/base/strimzi-operator/release.yaml
check_pin platform.kafka_broker deploy/base/kafka/kafka.yaml
check_pin images.seaweedfs deploy/base/seaweedfs/statefulset.yaml
check_pin images.seaweedfs deploy/base/seaweedfs/buckets-job.yaml
check_pin images.postgres deploy/base/postgres/statefulset.yaml
check_pin images.prometheus deploy/base/prometheus/statefulset.yaml
check_pin images.grafana deploy/base/grafana/deployment.yaml
check_pin images.jre jvm/ingestor/Dockerfile

if [ "$failed" -ne 0 ]; then
  echo "deploy validation failed" >&2
  exit 1
fi
echo "deploy manifests are valid"
