package dev.wikiguard.stream.lake

import java.time.Instant

/** Row of bronze.events_raw. Field names match the column names. */
final case class BronzeRow(
    wiki: Option[String],
    stream: Option[String],
    event_id: Option[String],
    event_ts: Instant,
    ingested_at: Instant,
    json: String,
    kafka_topic: String,
    kafka_partition: Int,
    kafka_offset: Long
)
