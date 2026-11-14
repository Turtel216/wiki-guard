-- Upstream events exactly as the ingestor stored them, one row per event.
-- This is the copy of the raw Kafka topics that outlives their retention.

CREATE NAMESPACE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.events_raw (
  wiki STRING COMMENT 'Database name of the wiki, such as enwiki. Null when the body could not be read.',
  stream STRING COMMENT 'Upstream stream the event came from. Null when the body could not be read.',
  event_id STRING COMMENT 'Position of the event in the upstream stream.',
  event_ts TIMESTAMP COMMENT 'When the event happened upstream. The ingest time when the event has no time of its own.',
  ingested_at TIMESTAMP COMMENT 'When the ingestor stored the event in Kafka.',
  json STRING COMMENT 'The event body, untouched.',
  kafka_topic STRING COMMENT 'Raw topic the event was read from.',
  kafka_partition INT COMMENT 'Partition of that topic.',
  kafka_offset BIGINT COMMENT 'Offset in that partition. Topic, partition and offset together identify a row.'
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
  'format-version' = '2',
  'write.parquet.compression-codec' = 'zstd'
);
