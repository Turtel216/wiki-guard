package dev.wikiguard.stream.lake

import java.time.Instant

/** A record as the Kafka source delivers it, with the columns the lake needs. */
final case class KafkaRecord(
    value: Option[Array[Byte]],
    topic: String,
    partition: Int,
    offset: Long,
    timestamp: Instant,
    headers: Option[Seq[KafkaHeader]]
)
