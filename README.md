# WikiGuard

WikiGuard scores every human edit to English Wikipedia articles for revert
risk within seconds, then grades itself against the community's reverts.
It is read-only and never edits Wikipedia.

Status: early scaffolding. Nothing connects to Wikimedia yet.

## A note on scale

English Wikipedia gets about 2 edits per second. The live stream alone does
not need a cluster. Spark is here for three other jobs: backfilling and
training on months of history, replaying the stream at hundreds of times
live speed, and stateful per-user and per-page processing written the way a
larger deployment would write it.

## Layout

```text
proto/       Kafka contracts (Protobuf) and buf config
jvm/         sbt multi-project, Scala 2.13
  common/          shared config, metrics, Kafka and S3 helpers
  features-core/   pure feature functions, no Spark
  ingestor/        EventStreams to Kafka
  enricher/        rate-limited Wikimedia API client
  stream/          Structured Streaming queries
  batch/           backfill, labels, evaluation, table maintenance
  integration/     tests that start containers
ml/          Python: dataset, training, evaluation, promotion
sql/ddl/     Iceberg and PostgreSQL DDL
deploy/      Kustomize bases, overlays and the Flux entry point
scripts/     cluster setup, dev cluster and manifest checks
workflows/   Argo templates and CronWorkflows
dashboards/  Grafana JSON
fixtures/    small recorded event samples
config/      Wikimedia limits, budgets and User-Agent
docs/        ADRs and runbooks
```

## Requirements

- JDK 21
- [sbt](https://www.scala-sbt.org/)
- [uv](https://docs.astral.sh/uv/)
- make
- Docker, for the integration tests and the dev cluster
- [k3d](https://k3d.io/) and kubectl, for the dev cluster only
- [kubeconform](https://github.com/yannh/kubeconform), for `make lint-deploy` only

## Getting started

```sh
make help               # list targets
make test               # Scala and Python unit tests
make integration-test   # tests against SeaweedFS and PostgreSQL containers
make lint               # formatting, lint rules and types
make fmt                # format and auto-fix
make lint-deploy        # check the manifests under deploy/
```

## Dev cluster

```sh
make dev-up     # k3s in Docker with Kafka, SeaweedFS and PostgreSQL
make dev-down   # delete it again
```

The dev cluster is local and holds fixture data only. It is reached
through the `k3d-wikiguard-dev` kubectl context:

```sh
kubectl --context k3d-wikiguard-dev -n platform get pods
```

Setting up the real server is described in
[docs/runbooks/server-bootstrap.md](docs/runbooks/server-bootstrap.md).

Versions for the whole stack are pinned in `versions.toml`.
