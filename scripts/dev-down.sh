#!/usr/bin/env bash
# Delete the local dev cluster and everything stored in it.
set -euo pipefail

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need k3d

k3d cluster delete wikiguard-dev
