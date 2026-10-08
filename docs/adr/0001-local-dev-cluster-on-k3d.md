# ADR 0001: Local dev cluster on k3d

Date: 2026-11-15

Status: accepted

## Context

`make dev-up` must bring up a development environment with one command.
The original plan was a `wikiguard-dev` namespace pinned to the desktop,
which only works once the desktop has joined the server's cluster as an
agent. That happens late in the project, and it makes development depend
on the server being up.

## Decision

`make dev-up` creates a k3s cluster inside Docker with k3d, on whatever
machine it is run on. It installs the Flux controllers and applies
`deploy/overlays/dev` from the working tree. The k3s version is the one
pinned for the server.

Every kubectl call in the dev scripts names the `k3d-wikiguard-dev`
context, so they cannot touch the home server.

## Alternatives

- A dev namespace on the server's cluster. Needs the server and a joined
  desktop, and a mistake in dev can hurt production.
- Docker Compose. Fast, but the manifests would not be exercised before
  they reach the server.

## Consequences

- Development works without the server and without any network path to it.
- The dev cluster runs the same bases as production, so a broken manifest
  shows up locally first.
- The dev overlay is applied with kubectl, not reconciled from Git, so
  uncommitted changes can be tried. Flux's Git sync itself is only
  exercised on the server.
- Developers need Docker, k3d and kubectl.
