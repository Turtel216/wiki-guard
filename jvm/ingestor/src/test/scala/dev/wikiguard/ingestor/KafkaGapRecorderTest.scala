package dev.wikiguard.ingestor

import java.time.{Clock, Instant, ZoneOffset}

import scala.jdk.CollectionConverters._

import org.apache.kafka.clients.producer.{MockProducer, RoundRobinPartitioner}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.scalatest.funsuite.AnyFunSuite

import dev.wikiguard.contracts.v1.{IngestGap => IngestGapMessage}

class KafkaGapRecorderTest extends AnyFunSuite {

  private val Topic = "wg.ops.ingest-gaps.v1"
  private val Now = Instant.parse("2026-11-10T12:00:00.250Z")
  private val gap =
    IngestGap(Instant.parse("2026-11-01T08:00:00Z"), Instant.parse("2026-11-03T12:00:05Z"))

  private def producer(autoComplete: Boolean = true): MockProducer[String, Array[Byte]] =
    new MockProducer(
      autoComplete,
      new RoundRobinPartitioner,
      new StringSerializer,
      new ByteArraySerializer
    )

  private def recorder(kafka: MockProducer[String, Array[Byte]], wikis: Set[String]) =
    new KafkaGapRecorder(kafka, Topic, wikis, Clock.fixed(Now, ZoneOffset.UTC))

  private def seconds(instant: Instant): Long = instant.getEpochSecond

  test("a gap is published as a message that reads back with every field") {
    val kafka = producer()

    assert(recorder(kafka, Set("enwiki")).record(gap) == Right(()))

    val sent = kafka.history.asScala.toVector
    assert(sent.map(_.topic) == Vector(Topic))
    assert(sent.map(_.key) == Vector("enwiki"))
    val messages = sent.map(record => IngestGapMessage.parseFrom(record.value))
    assert(messages.map(_.getWiki) == Vector("enwiki"))
    assert(messages.map(_.getGapStart.getSeconds) == Vector(seconds(gap.from)))
    assert(messages.map(_.getGapEnd.getSeconds) == Vector(seconds(gap.to)))
    assert(messages.map(_.getDetectedAt.getSeconds) == Vector(seconds(Now)))
    assert(messages.map(_.getDetectedAt.getNanos) == Vector(250000000))
  }

  test("every followed wiki gets its own message") {
    val kafka = producer()

    recorder(kafka, Set("enwiki", "dewiki")).record(gap)

    assert(kafka.history.asScala.toVector.map(_.key) == Vector("dewiki", "enwiki"))
  }

  test("a message the broker refuses is reported") {
    val kafka = producer(autoComplete = false)
    val refusal = new IllegalStateException("broker said no")
    val broker = new Thread(() => {
      Iterator.continually { Thread.sleep(5); kafka.errorNext(refusal) }.exists(done => done)
      ()
    })
    broker.start()

    val result = recorder(kafka, Set("enwiki")).record(gap)

    assert(result.left.exists(_.message.contains("broker said no")))
  }
}
