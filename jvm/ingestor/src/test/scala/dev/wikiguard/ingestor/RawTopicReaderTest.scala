package dev.wikiguard.ingestor

import java.time.Duration

import scala.jdk.CollectionConverters._

import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

import dev.wikiguard.ingestor.MockRawTopics.Stored

class RawTopicReaderTest extends AnyFunSuite {

  private val Revisions = "wg.raw.revision-create"
  private val Tags = "wg.raw.revision-tags-change"
  private val Topics = Set(Revisions, Tags)

  private def reader(stored: Seq[Stored], topics: Set[String] = Topics): RawTopicReader =
    new RawTopicReader(
      () => MockRawTopics.consumer(Topics, stored),
      topics,
      Duration.ofMillis(200)
    )

  /** Ten events dealt out over three partitions, numbered in the order they were written. */
  private val ten: Vector[Stored] = (1 to 10).toVector.map { number =>
    val (topic, partition) = Vector((Revisions, 0), (Revisions, 1), (Tags, 0))(number % 3)
    Stored(
      topic,
      partition,
      offset = ((number - 1) / 3).toLong,
      writtenAt = 1000L + number,
      eventId = Some(s"[$number]"),
      body = SampleEvents.body(pageId = number.toLong)
    )
  }

  private def ids(events: Either[RecordingError, Vector[StreamEvent]]) =
    events.map(_.flatMap(_.id))

  test("empty topics give an empty recording") {
    assert(reader(Nil).newest(5) == Right(Vector.empty))
  }

  test("events from all partitions come back in the order they were written") {
    assert(ids(reader(ten).newest(100)) == Right((1 to 10).map(n => s"[$n]")))
  }

  test("only the newest events are kept when there are more than asked for") {
    assert(ids(reader(ten).newest(4)) == Right((7 to 10).map(n => s"[$n]")))
  }

  test("the newest events are found even when they all sit in one partition") {
    val lopsided = (1 to 6).toVector.map { number =>
      Stored(Revisions, 0, offset = (number - 1).toLong, 1000L + number, Some(s"[$number]"))
    } :+ Stored(Tags, 1, offset = 0, writtenAt = 500L, eventId = Some("[old]"))

    assert(ids(reader(lopsided).newest(3)) == Right(Vector("[4]", "[5]", "[6]")))
  }

  test("an event comes back with its body and stream position untouched") {
    val body = SampleEvents.body(pageId = 99L)
    val stored = Stored(Revisions, 0, offset = 0, writtenAt = 1000L, Some("[1]"), body)

    assert(reader(Seq(stored)).newest(1) == Right(Vector(StreamEvent(Some("[1]"), body))))
  }

  test("a record without a stream position becomes an event without an id") {
    val stored = Stored(Revisions, 0, offset = 0, writtenAt = 1000L, eventId = None)

    assert(reader(Seq(stored)).newest(1).map(_.map(_.id)) == Right(Vector(None)))
  }

  test("a missing topic is an error") {
    val result = reader(Nil, Topics + "wg.raw.unknown").newest(5)

    assert(result.left.exists(_.message.contains("wg.raw.unknown")))
  }

  test("records that never arrive are an error, not a short recording") {
    val stuck = new RawTopicReader(
      () => {
        // The broker says there are five records but never hands any over.
        val consumer = MockRawTopics.consumer(Topics, Nil)
        consumer.updateEndOffsets(Map(new TopicPartition(Revisions, 0) -> Long.box(5L)).asJava)
        consumer
      },
      Topics,
      Duration.ofMillis(200)
    )

    assert(stuck.newest(5).left.exists(_.message.contains("timed out")))
  }
}
