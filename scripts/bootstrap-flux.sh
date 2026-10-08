#!/usr/bin/env bash
# Connect the home cluster to this repository with Flux. Run it once, with
# kubectl pointing at the home server, after the encrypted secrets have
# been merged to the main branch.
#
# Flux pushes its own manifests to deploy/clusters/home/flux-system on the
# main branch, so the token needs write access to the repository:
#   GITHUB_TOKEN=... scripts/bootstrap-flux.sh
set -euo pipefail

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need kubectl flux

GITHUB_OWNER="${GITHUB_OWNER:-Turtel216}"
GITHUB_REPO="${GITHUB_REPO:-wiki-guard}"
AGE_KEY="${WG_AGE_KEY:-$HOME/.config/wikiguard/prod.agekey}"
FLUX_VERSION="$(pinned platform.flux)"

if [ -z "${GITHUB_TOKEN:-}" ]; then
  echo "set GITHUB_TOKEN to a token that can write to $GITHUB_OWNER/$GITHUB_REPO" >&2
  exit 1
fi
if [ ! -f "$AGE_KEY" ]; then
  echo "no age key at $AGE_KEY, run 'make prod-secrets' first or set WG_AGE_KEY" >&2
  exit 1
fi

context="$(kubectl config current-context)"
case "$context" in
  k3d-*)
    echo "the current context is $context, a local dev cluster; point kubectl at the server" >&2
    exit 1
    ;;
esac

if ! flux --version | grep -qF "${FLUX_VERSION#v}"; then
  echo "the flux command is not the pinned version $FLUX_VERSION" >&2
  exit 1
fi

echo "== bootstrapping Flux $FLUX_VERSION on context $context"
flux check --pre

# Flux decrypts the secrets in Git with this key.
kubectl create namespace flux-system --dry-run=client -o yaml | kubectl apply -f -
kubectl -n flux-system create secret generic sops-age \
  --from-file=age.agekey="$AGE_KEY" --dry-run=client -o yaml | kubectl apply -f -

flux bootstrap github \
  --owner "$GITHUB_OWNER" \
  --repository "$GITHUB_REPO" \
  --branch main \
  --path deploy/clusters/home \
  --personal \
  --version "$FLUX_VERSION"

flux get kustomizations
