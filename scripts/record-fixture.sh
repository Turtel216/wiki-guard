#!/usr/bin/env bash
# Save the newest events from the raw Kafka topics as a recording that the
# ingestor can replay.
#
#   scripts/record-fixture.sh fixtures/my-recording.sse [number of events]
#
# The recorder runs in a short-lived pod next to the ingestor and uses the
# same image. It reads Kafka only and never contacts Wikimedia, so it is
# safe while the ingestor is live.
#
# The dev cluster is used unless KUBE_CONTEXT names another one.
set -euo pipefail

. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
need kubectl

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
  echo "usage: $0 <output file> [number of events]" >&2
  exit 1
fi

OUTPUT="$1"
MAX_EVENTS="${2:-1000}"
CONTEXT="${KUBE_CONTEXT:-k3d-wikiguard-dev}"
NAMESPACE="wikiguard"
POD="fixture-recorder"
RECORDING="/tmp/recording.sse"

kc() {
  kubectl --context "$CONTEXT" -n "$NAMESPACE" "$@"
}

# The recorder must match the ingestor that wrote the topics, so both the
# image and the broker address are taken from its deployment.
IMAGE="$(kc get deployment ingestor \
  -o jsonpath='{.spec.template.spec.containers[0].image}')"
KAFKA="$(kc get deployment ingestor \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="WG_KAFKA_BOOTSTRAP_SERVERS")].value}')"

cleanup() {
  kc delete pod "$POD" --ignore-not-found --wait=false >/dev/null
}
trap cleanup EXIT

echo "== starting pod $POD in $CONTEXT with $IMAGE"
kc delete pod "$POD" --ignore-not-found >/dev/null
# The pod only waits. The recorder is started inside it below, so that the
# recording can be copied out before the pod goes away.
kc apply -f - <<MANIFEST
apiVersion: v1
kind: Pod
metadata:
  name: $POD
  labels:
    app.kubernetes.io/name: fixture-recorder
spec:
  restartPolicy: Never
  activeDeadlineSeconds: 900
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
  containers:
    - name: recorder
      image: $IMAGE
      command: ["sleep", "900"]
      resources:
        requests:
          memory: 400Mi
        limits:
          memory: 400Mi
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
      volumeMounts:
        - name: tmp
          mountPath: /tmp
  volumes:
    - name: tmp
      emptyDir: {}
MANIFEST
kc wait pod "$POD" --for=condition=Ready --timeout=2m

echo "== recording up to $MAX_EVENTS events"
kc exec "$POD" -- env \
  WG_KAFKA_BOOTSTRAP_SERVERS="$KAFKA" \
  WG_RECORD_FILE="$RECORDING" \
  WG_RECORD_MAX_EVENTS="$MAX_EVENTS" \
  java -XX:MaxRAMPercentage=70 -cp '/opt/wikiguard/lib/*' \
  dev.wikiguard.ingestor.FixtureRecorderApp

mkdir -p "$(dirname "$OUTPUT")"
kc exec "$POD" -- cat "$RECORDING" >"$OUTPUT"

echo "== wrote $OUTPUT"
head -n 2 "$OUTPUT"
