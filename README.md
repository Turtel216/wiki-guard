# WikiGuard

WikiGuard scores every human edit to English Wikipedia articles for revert
risk within seconds, then grades itself against the community's reverts.
It is read-only and never edits Wikipedia.

Status: early. The ingestor exists but is not deployed, and nothing connects
to Wikimedia yet.

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
make record-fixture     # save the newest raw events as a replayable recording
```

## Dev cluster

```sh
make dev-up     # k3s in Docker with Kafka, SeaweedFS, PostgreSQL and the ingestor
make dev-down   # delete it again
```

The dev cluster is local and holds fixture data only. It is reached
through the `k3d-wikiguard-dev` kubectl context:

```sh
kubectl --context k3d-wikiguard-dev -n platform get pods
```

## Ingestor

The ingestor copies events from one stream connection into the raw Kafka
topics. It keeps no state of its own: on every connect it reads the last
record it wrote and resumes right after it. If that record is older than
what upstream still keeps, the lost stretch is published to
`wg.ops.ingest-gaps.v1` before anything else is stored. It is configured through
environment variables:

| Variable | Meaning | Default |
| --- | --- | --- |
| `WG_SOURCE` | `live` for the Wikimedia stream, `fixture` for a recording | none, must be set |
| `WG_KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | none, must be set |
| `WG_FIXTURE_FILE` | Recording to replay, as raw server-sent events text | none |
| `WG_REPLAY_SPEED` | How many times faster than real life to replay | `1` |
| `WG_WIKIMEDIA_CONFIG` | Path to the Wikimedia config file | `config/wikimedia.yaml` |
| `WG_METRICS_PORT` | Port that serves `/metrics` | `9400` |
| `WG_STAY_UP_AFTER_REPLAY` | `true` keeps the process up once a recording has ended | `false` |

The `live` source refuses to start until `user_agent.contact` is filled in
in `config/wikimedia.yaml`.

`make dev-up` also builds the ingestor image from the working tree and
loads it into the cluster, where it replays a small hand-written sample
into the raw topics. Run it again to pick up code changes. Pods in the
`wikiguard` namespace cannot reach anything outside the cluster.

## Fixture recorder

Development and tests replay recordings and never contact Wikimedia. A
recording is made from the raw Kafka topics, where the ingestor has already
stored the events, so recording needs no second stream connection:

```sh
make record-fixture FILE=fixtures/my-recording.sse EVENTS=2000
```

This starts a short-lived pod next to the ingestor, saves the newest events
in the order the stream delivered them, and copies the file out. The dev
cluster is used unless `KUBE_CONTEXT` names another one. The result is
server-sent events text, the format `WG_FIXTURE_FILE` expects.

A recording holds only what the ingestor kept. Canary events and other
wikis are dropped before Kafka, so they are not in it.

| Variable | Meaning | Default |
| --- | --- | --- |
| `WG_KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | none, must be set |
| `WG_RECORD_FILE` | Where to write the recording; an existing file is replaced | none, must be set |
| `WG_RECORD_MAX_EVENTS` | How many of the newest events to record, up to 50000 | `1000` |
| `WG_WIKIMEDIA_CONFIG` | Path to the Wikimedia config file | `config/wikimedia.yaml` |

Recorded events are real edits and name their editors. They are public, but
keep committed recordings small.

Setting up the real server is described in
[docs/runbooks/server-bootstrap.md](docs/runbooks/server-bootstrap.md).

Versions for the whole stack are pinned in `versions.toml`.
