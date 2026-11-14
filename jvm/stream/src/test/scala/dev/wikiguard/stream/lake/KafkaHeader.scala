package dev.wikiguard.stream.lake

/** One header of a Kafka record, the way the Kafka source delivers it. */
final case class KafkaHeader(key: String, value: Array[Byte])
