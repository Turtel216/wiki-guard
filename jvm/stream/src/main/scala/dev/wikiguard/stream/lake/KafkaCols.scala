package dev.wikiguard.stream.lake

/** Columns of a DataFrame read from Kafka, as the Kafka source names them. */
object KafkaCols {

  val Value: String = "value"
  val Topic: String = "topic"
  val Partition: String = "partition"
  val Offset: String = "offset"
  val Timestamp: String = "timestamp"
  val Headers: String = "headers"

  /** Fields of one entry of the headers column. */
  val HeaderKey: String = "key"
  val HeaderValue: String = "value"
}
