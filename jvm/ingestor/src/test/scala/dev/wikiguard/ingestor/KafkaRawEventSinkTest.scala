package dev.wikiguard.ingestor
import java.time.{Clock, Instant, ZoneOffset}

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.producer.{MockProducer, RoundRobinPartitioner}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.funsuite.AnyFunSuite

/** Uses the in-memory producer and consumer that ship with the Kafka client. */
class KafkaRawEventSinkTest extends AnyFunSuite {

  import MockRawTopics.Stored

  private val Revisions = "wg.raw.revision-create"
  private val Tags = "wg.raw.revision-tags-change"
  private val Topics = Set(Revisions, Tags)

  // A clock that stands still, so every write falls into the same millisecond.
  private val clock = Clock.fixed(Instant.ofEpochMilli(1000L), ZoneOffset.UTC)

  private def producer(autoComplete: Boolean = true): MockProducer[String, String] =
    new MockProducer(
      autoComplete,
      new RoundRobinPartitioner,
      new StringSerializer,
      new StringSerializer
    )

  private def record(pageId: Option[Long] = Some(7L), eventId: Option[String] = Some("[1]")) =
    RawRecord(Revisions, pageId, eventId, None, SampleEvents.body())

  private def consumer(stored: Seq[Stored]): MockConsumer[String, String] =
    MockRawTopics.consumer(Topics, stored)

  private def lastEvent(stored: Stored*): Either[SinkError, Option[ResumePoint]] =
    new KafkaRawEventSink(producer(), () => consumer(stored), Topics, clock).lastEvent()

  test("a record is keyed by page id and carries the body and the stream position") {
    val kafka = producer()
    val sink = new KafkaRawEventSink(kafka, () => consumer(Nil), Topics, clock)

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
    val sink = new KafkaRawEventSink(kafka, () => consumer(Nil), Topics, clock)

    assert(sink.write(record(pageId = None, eventId = None)) == Right(()))

    val sent = kafka.history.asScala.toVector
    assert(sent.map(message => Option(message.key)) == Vector(None))
    assert(sent.forall(_.headers.toArray.isEmpty))
  }

  test("no two records get the same timestamp, even within one millisecond") {
    val kafka = producer()
    val sink = new KafkaRawEventSink(kafka, () => consumer(Nil), Topics, clock)

    (1 to 3).foreach(_ => sink.write(record()))

    assert(kafka.history.asScala.toVector.map(_.timestamp.toLong) == Vector(1000L, 1001L, 1002L))
  }

  test("a write the broker refuses is reported") {
    val kafka = producer(autoComplete = false)
    val sink = new KafkaRawEventSink(kafka, () => consumer(Nil), Topics, clock)
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
    val sink =
      new KafkaRawEventSink(producer(), () => consumer(Nil), Topics + "wg.raw.unknown", clock)

    assert(sink.lastEvent().left.exists(_.message.contains("wg.raw.unknown")))
  }
}
