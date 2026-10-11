package dev.wikiguard.ingestor

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.Optional

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.{ConsumerRecord, MockConsumer}
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy
import org.apache.kafka.clients.producer.{MockProducer, RoundRobinPartitioner}
import org.apache.kafka.common.{Node, PartitionInfo, TopicPartition}
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.funsuite.AnyFunSuite

/** Uses the in-memory producer and consumer that ship with the Kafka client. */
class KafkaRawEventSinkTest extends AnyFunSuite {

  import KafkaRawEventSinkTest.Stored

  private val Revisions = "wg.raw.revision-create"
  private val Tags = "wg.raw.revision-tags-change"
  private val Topics = Set(Revisions, Tags)

  private def producer(autoComplete: Boolean = true): MockProducer[String, String] =
    new MockProducer(
      autoComplete,
      new RoundRobinPartitioner,
      new StringSerializer,
      new StringSerializer
    )

  private def record(pageId: Option[Long] = Some(7L), eventId: Option[String] = Some("[1]")) =
    RawRecord(Revisions, pageId, eventId, None, SampleEvents.body())

  /** A consumer over two partitions per topic that holds the given records. */
  private def consumer(stored: Seq[Stored]): MockConsumer[String, String] = {
    val mock = new MockConsumer[String, String](AutoOffsetResetStrategy.EARLIEST.name)
    val partitions = for {
      topic <- Topics.toVector
      partition <- 0 to 1
    } yield new TopicPartition(topic, partition)
    Topics.foreach { topic =>
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
      stored.filter(s =>
        mock.assignment.contains(new TopicPartition(s.topic, s.partition))
      ).foreach {
        s => mock.addRecord(consumerRecord(s))
      }
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

  private def lastEvent(stored: Stored*): Either[SinkError, Option[ResumePoint]] =
    new KafkaRawEventSink(producer(), () => consumer(stored), Topics).lastEvent()

  test("a record is keyed by page id and carries the body and the stream position") {
    val kafka = producer()
    val sink = new KafkaRawEventSink(kafka, () => consumer(Nil), Topics)

    assert(sink.write(record()) == Right(()))

    val sent = kafka.history.asScala.toVector
    assert(sent.map(_.topic) == Vector(Revisions))
    assert(sent.map(_.key) == Vector("7"))
    assert(sent.map(_.value) == Vector(SampleEvents.body()))
    val positions = sent.flatMap(_.headers.headers(KafkaRawEventSink.EventIdHeader).asScala)
    assert(positions.map(header => new String(header.value, "UTF-8")) == Vector("[1]"))
  }

  test("an event without a page id or a position is still written") {
    val kafka = producer()
    val sink = new KafkaRawEventSink(kafka, () => consumer(Nil), Topics)

    assert(sink.write(record(pageId = None, eventId = None)) == Right(()))

    val sent = kafka.history.asScala.toVector
    assert(sent.map(message => Option(message.key)) == Vector(None))
    assert(sent.forall(_.headers.toArray.isEmpty))
  }

  test("a write the broker refuses is reported") {
    val kafka = producer(autoComplete = false)
    val sink = new KafkaRawEventSink(kafka, () => consumer(Nil), Topics)
    // The mock answers on another thread, the way a broker would.
    val refusal = new IllegalStateException("broker said no")
    val broker = new Thread(() => {
      Iterator.continually { Thread.sleep(5); kafka.errorNext(refusal) }.exists(done => done)
      ()
    })
    broker.start()

    val result = sink.write(record())

    assert(result.left.exists(_.message.contains("broker said no")))
  }

  test("empty topics have no last event") {
    assert(lastEvent() == Right(None))
  }

  test("the last event is the most recently written record across all partitions") {
    val result = lastEvent(
      Stored(Revisions, 0, offset = 4, writtenAt = 1000, eventId = Some("old")),
      Stored(Revisions, 1, offset = 9, writtenAt = 3000, eventId = Some("newest")),
      Stored(Tags, 0, offset = 2, writtenAt = 2000, eventId = Some("middle"))
    )

    assert(result.map(_.map(_.eventId)) == Right(Some("newest")))
  }

  test("the resume point takes its time from the event itself") {
    val body = SampleEvents.body(time = "2026-11-01T10:00:00Z")

    val result = lastEvent(Stored(Revisions, 0, 0, writtenAt = 5000, Some("[1]"), body))

    assert(result.map(_.map(_.eventTime)) == Right(Some(Instant.parse("2026-11-01T10:00:00Z"))))
  }

  test("an event without a time of its own uses the time it was written") {
    val result = lastEvent(Stored(Revisions, 0, 0, writtenAt = 5000, Some("[1]"), "{}"))

    assert(result.map(_.map(_.eventTime)) == Right(Some(Instant.ofEpochMilli(5000))))
  }

  test("records without a stream position are skipped") {
    val result = lastEvent(
      Stored(Revisions, 0, offset = 0, writtenAt = 1000, eventId = Some("has-position")),
      Stored(Revisions, 1, offset = 0, writtenAt = 2000, eventId = None)
    )

    assert(result.map(_.map(_.eventId)) == Right(Some("has-position")))
  }

  test("a missing topic is an error") {
    val sink = new KafkaRawEventSink(producer(), () => consumer(Nil), Topics + "wg.raw.unknown")

    assert(sink.lastEvent().left.exists(_.message.contains("wg.raw.unknown")))
  }
}

object KafkaRawEventSinkTest {

  /** A stored record as the consumer would return it. */
  final case class Stored(
      topic: String,
      partition: Int,
      offset: Long,
      writtenAt: Long,
      eventId: Option[String],
      body: String = SampleEvents.body()
  )
}
