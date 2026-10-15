package dev.wikiguard.ingestor

import java.time.Duration

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

/**
 * Reads the newest events back out of the raw topics, in the order the stream delivered them.
 *
 * This is how a recording is made without a second connection to the stream: the events are taken
 * from where the ingestor already put them. Only events the ingestor kept can be read back, so a
 * recording made this way holds no canaries and no other wikis.
 *
 * @param newConsumer
 *   opens a consumer for one read; it is closed afterwards
 * @param topics
 *   the raw topics to read from
 * @param patience
 *   how long to wait for the broker to send something before giving up
 */
final class RawTopicReader(
    newConsumer: () => Consumer[String, String],
    topics: Set[String],
    patience: Duration
) {

  import RawTopicReader.Range

  /** The latest `count` events over all topics, oldest first. Fewer if there are not that many. */
  def newest(count: Int): Either[RecordingError, Vector[StreamEvent]] =
    Try {
      Using.resource(newConsumer()) { consumer =>
        val ranges = tails(consumer, count)
        consumer.assign(ranges.keySet.asJava)
        ranges.foreach { case (partition, range) => consumer.seek(partition, range.from) }
        val deadline = System.nanoTime() + patience.toNanos
        // The sink never repeats a timestamp, so sorting by it gives back the stream order.
        read(consumer, ranges, Vector.empty, deadline)
          .sortBy(_.timestamp)
          .takeRight(count)
          .flatMap(toEvent)
      }
    }.toEither.left.map(error => RecordingError.ReadFailed(error.toString))

  /**
   * The last `count` offsets of every partition that holds anything. The newest events overall
   * could all sit in one partition, so that many are read from each.
   */
  private def tails(consumer: Consumer[String, String], count: Int) = {
    val partitions = topics.toVector.flatMap(RawTopicAccess.partitionsOf(consumer, _))
    val starts = consumer.beginningOffsets(partitions.asJava).asScala
    consumer
      .endOffsets(partitions.asJava)
      .asScala
      .map { case (partition, end) =>
        partition -> Range(math.max(starts(partition), end - count), end)
      }
      .filter { case (_, range) => range.from < range.until }
      .toMap
  }

  /** Polls until every partition has been read up to the end it had when the read began. */
  @tailrec
  private def read(
      consumer: Consumer[String, String],
      ranges: Map[TopicPartition, Range],
      found: Vector[ConsumerRecord[String, String]],
      deadline: Long
  ): Vector[ConsumerRecord[String, String]] =
    if (ranges.forall { case (partition, range) => consumer.position(partition) >= range.until }) {
      found
    } else if (System.nanoTime() > deadline) {
      throw new IllegalStateException("timed out waiting for records")
    } else {
      val batch = consumer.poll(RawTopicReader.PollTime).asScala.toVector.filter { record =>
        ranges.get(new TopicPartition(
          record.topic,
          record.partition
        )).exists(_.holds(record.offset))
      }
      // Any progress buys more time, so a long read is fine as long as it keeps moving.
      val nextDeadline = if (batch.isEmpty) deadline else System.nanoTime() + patience.toNanos
      read(consumer, ranges, found ++ batch, nextDeadline)
    }

  private def toEvent(record: ConsumerRecord[String, String]): Option[StreamEvent] =
    Option(record.value).map(body => StreamEvent(RawTopicAccess.eventId(record), body))
}

object RawTopicReader {

  private val PollTime = Duration.ofMillis(500)

  private val DefaultPatience = Duration.ofSeconds(30)

  /** Offsets from `from` up to but not including `until`. */
  final private case class Range(from: Long, until: Long) {
    def holds(offset: Long): Boolean = offset >= from && offset < until
  }

  def apply(bootstrapServers: String, topics: Set[String]): RawTopicReader = {
    val settings = RawTopicAccess.consumerSettings(bootstrapServers, "wikiguard-fixture-recorder")
    def newConsumer(): Consumer[String, String] =
      new KafkaConsumer(settings, new StringDeserializer, new StringDeserializer)
    new RawTopicReader(() => newConsumer(), topics, DefaultPatience)
  }
}
