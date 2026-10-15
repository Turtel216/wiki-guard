package dev.wikiguard.ingestor

import java.nio.charset.StandardCharsets
import java.util.Properties

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition

/** What everything that reads the raw topics back has in common. */
private[ingestor] object RawTopicAccess {

  /** Fails for a topic that does not exist, so that it is never mistaken for an empty one. */
  def partitionsOf(consumer: Consumer[String, String], topic: String): Vector[TopicPartition] = {
    val partitions = Option(consumer.partitionsFor(topic)).fold(Vector.empty[TopicPartition]) {
      infos => infos.asScala.toVector.map(info => new TopicPartition(topic, info.partition))
    }
    if (partitions.isEmpty) throw new IllegalStateException(s"topic $topic does not exist")
    partitions
  }

  /** The stream position that was stored with a record, if it has one. */
  def eventId(record: ConsumerRecord[String, String]): Option[String] =
    Option(record.headers.lastHeader(KafkaRawEventSink.EventIdHeader))
      .flatMap(header => Option(header.value))
      .map(bytes => new String(bytes, StandardCharsets.UTF_8))

  /** For a consumer that reads fixed offsets, so it joins no group and commits nothing. */
  def consumerSettings(bootstrapServers: String, clientId: String): Properties = {
    val settings = new Properties()
    settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    settings.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
    settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    settings
  }
}
