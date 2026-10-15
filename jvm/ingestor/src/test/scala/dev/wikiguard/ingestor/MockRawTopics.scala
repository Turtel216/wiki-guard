package dev.wikiguard.ingestor

import java.nio.charset.StandardCharsets
import java.util.Optional

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer}
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy
import org.apache.kafka.common.{Node, PartitionInfo, TopicPartition}
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.record.TimestampType

/** Raw topics that live in the in-memory consumer that ships with the Kafka client. */
object MockRawTopics {

  /** A stored record as the consumer would return it. */
  final case class Stored(
      topic: String,
      partition: Int,
      offset: Long,
      writtenAt: Long,
      eventId: Option[String],
      body: String = SampleEvents.body()
  )

  /** A consumer over two partitions per topic that holds the given records. */
  def consumer(topics: Set[String], stored: Seq[Stored]): MockConsumer[String, String] = {
    val mock = new MockConsumer[String, String](AutoOffsetResetStrategy.EARLIEST.name)
    val partitions = for {
      topic <- topics.toVector
      partition <- 0 to 1
    } yield new TopicPartition(topic, partition)
    topics.foreach { topic =>
      val infos = (0 to 1).map(new PartitionInfo(topic, _, Node.noNode, Array.empty, Array.empty))
      mock.updatePartitions(topic, infos.asJava)
    }
    def ends(partition: TopicPartition): Long = stored
      .filter(s => s.topic == partition.topic && s.partition == partition.partition)
      .map(_.offset + 1)
      .maxOption
      .getOrElse(0L)
    mock.updateBeginningOffsets(partitions.map(_ -> Long.box(0L)).toMap.asJava)
    mock.updateEndOffsets(partitions.map(p => p -> Long.box(ends(p))).toMap.asJava)
    // Records can only be added to assigned partitions, so they are added on the first poll.
    mock.schedulePollTask { () =>
      stored
        .filter(s => mock.assignment.contains(new TopicPartition(s.topic, s.partition)))
        .foreach(s => mock.addRecord(consumerRecord(s)))
    }
    mock
  }

  private def consumerRecord(stored: Stored): ConsumerRecord[String, String] = {
    val headers = new RecordHeaders()
    stored.eventId.foreach { id =>
      headers.add(
        new RecordHeader(KafkaRawEventSink.EventIdHeader, id.getBytes(StandardCharsets.UTF_8))
      )
    }
    new ConsumerRecord(
      stored.topic,
      stored.partition,
      stored.offset,
      stored.writtenAt,
      TimestampType.CREATE_TIME,
      0,
      0,
      "7",
      stored.body,
      headers,
      Optional.empty[Integer]
    )
  }
}
