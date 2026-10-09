# Server bootstrap

How to turn a fresh home server into the WikiGuard cluster. Do the steps
in order. Each one is safe to repeat.

## Before you start

- The server runs a 64-bit Linux, has a reserved LAN address and at least
  250 GB of free SSD space.
- On the machine you run steps 2 and 4 from: `kubectl`, `flux`, `sops`,
  `age` and `openssl`. The `flux` version must match `platform.flux` in
  `versions.toml`.
- A GitHub token that can write to the repository. Flux uses it once, to
  push its own manifests and register a deploy key.

## 1. Install k3s

On the server, from a checkout of this repository:

```sh
sudo scripts/install-k3s.sh
```

Check: the node is `Ready` and carries the label `wikiguard.dev/tier=core`.

```sh
sudo k3s kubectl get nodes --show-labels
```

To use kubectl from another machine, copy `/etc/rancher/k3s/k3s.yaml`
from the server and replace `127.0.0.1` with the server's LAN address.

## 2. Generate the production secrets

```sh
make prod-secrets
```

This creates an age key at `~/.config/wikiguard/prod.agekey` and writes
encrypted credentials to `deploy/overlays/prod/secrets`.

- Commit the two files in `deploy/overlays/prod/secrets`.
- Back up the age key somewhere that is not this server. Without it the
  secrets cannot be read again.
- The script refuses to run twice. PostgreSQL keeps the password it was
  first started with, so a new one would lock everything out.

## 3. Merge to main

Flux deploys what is on the main branch. Merge the secrets and any
pending manifest changes before the next step.

## 4. Bootstrap Flux

With kubectl pointing at the server:

```sh
GITHUB_TOKEN=... scripts/bootstrap-flux.sh
```

Flux pushes a commit with its own manifests to
`deploy/clusters/home/flux-system` on main. Pull it afterwards.

## 5. Check the result

```sh
flux get kustomizations
kubectl -n platform get pods,kafka,job
```

Expected: the `operators`, `secrets` and `platform` kustomizations are
ready, the Kafka resource is ready, the `seaweedfs-buckets` job has
completed, and the `postgres` and `seaweedfs` pods are running.

## When something is stuck

- `flux get kustomizations` shows which stage failed and why.
- `platform` waits for `secrets`. If pods report a missing secret, step 2
  or 3 was skipped.
- `kubectl -n platform describe kafka wikiguard` shows what the Kafka
  operator is waiting for.
- A change to the bucket job is rejected because jobs cannot be edited.
  Delete the job and let Flux create it again:
  `kubectl -n platform delete job seaweedfs-buckets`.

## Upgrades

- k3s: change `platform.k3s` in `versions.toml`, then run step 1 again.
- Flux: change `platform.flux`, install the matching `flux` command, then
  run step 4 again.
- Everything else: change the version in the manifest and in
  `versions.toml`, and merge.
