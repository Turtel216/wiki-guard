#!/bin/sh
# Create the buckets that do not exist yet. Safe to run again.
set -eu

MASTER="seaweedfs-0.seaweedfs-peer:9333"
BUCKETS="wg-lake wg-checkpoints wg-mlflow wg-fixtures"

list_buckets() {
  echo "s3.bucket.list" | weed shell -master="$MASTER"
}

existing="$(list_buckets)"
for bucket in $BUCKETS; do
  if echo "$existing" | grep -q "$bucket"; then
    echo "bucket $bucket already exists"
  else
    echo "s3.bucket.create -name $bucket" | weed shell -master="$MASTER"
  fi
done

# The shell does not always fail loudly, so check the result.
existing="$(list_buckets)"
for bucket in $BUCKETS; do
  if ! echo "$existing" | grep -q "$bucket"; then
    echo "bucket $bucket is still missing" >&2
    exit 1
  fi
done
