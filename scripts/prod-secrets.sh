#!/usr/bin/env bash
# Generate the production credentials and write them, encrypted, to
# deploy/overlays/prod/secrets. Commit the result.
#
# The age key that can decrypt them is created on first use and stays
# outside the repository. Back it up: without it the secrets are lost.
set -euo pipefail

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need sops age-keygen openssl

AGE_KEY="${WG_AGE_KEY:-$HOME/.config/wikiguard/prod.agekey}"
OUT_DIR="$REPO_ROOT/deploy/overlays/prod/secrets"
OUT_FILE="$OUT_DIR/platform.enc.yaml"

# PostgreSQL only reads its password when it first creates its data
# directory, so replacing the file later would lock everything out.
if [ -e "$OUT_FILE" ]; then
  echo "$OUT_FILE already exists, refusing to replace it" >&2
  exit 1
fi

if [ ! -f "$AGE_KEY" ]; then
  echo "== creating a new age key at $AGE_KEY"
  mkdir -p "$(dirname "$AGE_KEY")"
  (umask 077 && age-keygen -o "$AGE_KEY")
fi
recipient="$(age-keygen -y "$AGE_KEY")"

random() {
  openssl rand -hex 24
}

plain="$(mktemp --suffix=.yaml)"
trap 'rm -f "$plain"' EXIT

cat > "$plain" <<YAML
apiVersion: v1
kind: Secret
metadata:
  name: postgres-credentials
  namespace: platform
stringData:
  password: $(random)
---
apiVersion: v1
kind: Secret
metadata:
  name: seaweedfs-s3
  namespace: platform
stringData:
  access_key: wikiguard
  secret_key: $(random)
YAML

# Only the values are encrypted, so the file still reads as a manifest.
sops encrypt --age "$recipient" --encrypted-regex '^(data|stringData)$' "$plain" > "$OUT_FILE"

cat > "$OUT_DIR/kustomization.yaml" <<YAML
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - platform.enc.yaml
YAML

echo "wrote ${OUT_FILE#"$REPO_ROOT"/}"
echo "commit deploy/overlays/prod/secrets and back up $AGE_KEY"
