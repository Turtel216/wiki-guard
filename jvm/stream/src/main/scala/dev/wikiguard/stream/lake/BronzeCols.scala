package dev.wikiguard.stream.lake

/** Columns of bronze.events_raw. */
object BronzeCols {

  val Wiki: String = "wiki"
  val Stream: String = "stream"
  val EventId: String = "event_id"
  val EventTs: String = "event_ts"
  val IngestedAt: String = "ingested_at"
  val Json: String = "json"
  val KafkaTopic: String = "kafka_topic"
  val KafkaPartition: String = "kafka_partition"
  val KafkaOffset: String = "kafka_offset"

  /** In the order of the table. */
  val All: Vector[String] = Vector(
    Wiki,
    Stream,
    EventId,
    EventTs,
    IngestedAt,
    Json,
    KafkaTopic,
    KafkaPartition,
    KafkaOffset
  )
}
