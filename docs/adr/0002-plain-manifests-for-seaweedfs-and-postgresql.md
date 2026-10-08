# ADR 0002: Plain manifests for SeaweedFS and PostgreSQL

Date: 2026-11-15

Status: accepted

## Context

The plan was to install every third-party component as a Flux HelmRelease.
For Kafka that fits: Strimzi is an operator and ships as a chart.

SeaweedFS and PostgreSQL are different. We run each as one process with
one volume. The SeaweedFS chart is built for a multi-component cluster and
its single-process mode is a small corner of a very large set of values.
PostgreSQL has no official chart; the usual choices are a third-party
chart or an operator that costs memory we have not budgeted.

## Decision

SeaweedFS and PostgreSQL are plain Kustomize bases: one StatefulSet and
its Services each, using the same images the integration tests use.
Strimzi stays a HelmRelease.

## Alternatives

- The SeaweedFS Helm chart in all-in-one mode. More moving parts than the
  thirty lines it replaces, and harder to review.
- CloudNativePG. Good backups and failover, but an extra always-on
  operator for a single small database.
- A third-party PostgreSQL chart. Adds a dependency on someone else's
  image and release schedule.

## Consequences

- Each manifest can be read top to bottom, and the cluster runs exactly
  what the integration tests run.
- Upgrades are an image tag change here and in `versions.toml`;
  `make lint-deploy` checks that the two agree.
- Backups and PostgreSQL major upgrades are our own job. Revisit if that
  becomes a burden.
