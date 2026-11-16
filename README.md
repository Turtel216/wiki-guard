# WikiGuard

WikiGuard scores every human edit to English Wikipedia articles for revert
risk within seconds, then grades itself against the community's reverts.
It is read-only and never edits Wikipedia.

Status: early. The ingestor and the query that copies raw events into the
lake exist but are not deployed, and nothing connects to Wikimedia yet.

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
make integration-test   # tests against Kafka, SeaweedFS and PostgreSQL containers
make lint               # formatting, lint rules and types
make fmt                # format and auto-fix
make lint-deploy        # check the manifests under deploy/
make record-fixture     # save the newest raw events as a replayable recording
```

## Dev cluster

```sh
make dev-up     # k3s in Docker with Kafka, SeaweedFS, PostgreSQL, the ingestor and the dashboards
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

## Lake sink

`lake-sink` is a query of the Spark streaming application. It copies the
raw Kafka topics into the Iceberg table `bronze.events_raw`, one row per
event, so the events outlive the seven days Kafka keeps them. The body is
stored untouched, next to the wiki, the stream name, the stream position,
the event time and the place in Kafka it was read from. The table is
partitioned by the day of the event.

A batch is committed about once a minute. After a crash the unfinished
batch is run again and the table takes it only once, so rows are neither
lost nor doubled as long as the checkpoint survives. Topic, partition and
offset identify a row if a checkpoint ever has to be thrown away.

The application is not deployed yet. It is configured through environment
variables; how Spark reaches the catalog and the object store is ordinary
Spark configuration.

| Variable | Meaning | Default |
| --- | --- | --- |
| `WG_KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | none, must be set |
| `WG_CHECKPOINT_ROOT` | Where the queries keep their checkpoints, such as `s3a://wg-checkpoints` | none, must be set |
| `WG_LAKE_CATALOG` | Name of the Iceberg catalog in the Spark configuration | `lake` |
| `WG_LAKE_SINK_TRIGGER_SECONDS` | How often a batch is appended to the lake | `60` |
| `WG_MAX_OFFSETS_PER_TRIGGER` | The most Kafka records one batch may take | `20000` |
| `WG_METRICS_PORT` | Port on which the driver serves `/metrics` | `9400` |

## Dashboards and metrics

Prometheus collects the metrics and Grafana shows them. Both run in the
`monitoring` namespace from plain manifests under `deploy/base`.

Grafana is a read-only viewer. Its data source and its dashboards come
from Git, nobody can log in, and it keeps nothing across restarts. A
dashboard is a JSON file in `dashboards/`; to change one, edit the file.
In the dev cluster it is reached through a port-forward:

```sh
kubectl --context k3d-wikiguard-dev -n monitoring port-forward service/grafana 3000:3000
```

Then open <http://localhost:3000>. On the server Grafana is served on the
LAN through the ingress that comes with k3s.

So far there is one dashboard, **Pipeline health**: how long ago the last
event was stored, what the ingestor wrote and dropped, how far each
streaming query is behind Kafka, and how much memory every pod uses.

Prometheus keeps seven days. It collects from every pod in the `wikiguard`
and `platform` namespaces that has a container port named `metrics`, so a
new service only has to name its port. Memory and CPU per container come
from the kubelets. All our own metrics start with `wg_`.

The ingestor reports under `wg_ingestor_`. The streaming application
listens to the progress of its queries and reports, per query:

| Metric | Meaning |
| --- | --- |
| `wg_stream_batches_total` | Batches finished |
| `wg_stream_input_rows_total` | Records read from the sources |
| `wg_stream_batch_duration_seconds` | How long the latest batch took |
| `wg_stream_source_lag_records` | Records waiting in each Kafka topic when the latest batch stopped reading |
| `wg_stream_last_progress_timestamp_seconds` | When the query last finished a batch or found nothing to do |

The streaming application is not deployed yet, so its panels stay empty.

## Lake tables

The tables are defined in `sql/ddl/iceberg`. The Spark job
`dev.wikiguard.batch.migrate.MigrateLake` runs every file there in the
order of the file names, against the catalog named by `WG_LAKE_CATALOG`
and the directory named by `WG_DDL_DIR`. Nothing records which files have
run, so every statement must be safe to run again, for example
`CREATE TABLE IF NOT EXISTS`. Tables are never changed by hand.

Setting up the real server is described in
[docs/runbooks/server-bootstrap.md](docs/runbooks/server-bootstrap.md).

Versions for the whole stack are pinned in `versions.toml`.
