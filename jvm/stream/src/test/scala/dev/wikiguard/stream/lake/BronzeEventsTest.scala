package dev.wikiguard.stream.lake

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant

import org.scalatest.funsuite.AnyFunSuite

import dev.wikiguard.common.kafka.RawTopicNames
import dev.wikiguard.stream.SparkTestSession

class BronzeEventsTest extends AnyFunSuite with SparkTestSession {

  import spark.implicits._

  private val Topic = RawTopicNames.RevisionCreate
  private val IngestedAt = Instant.parse("2026-11-01T10:00:03.250Z")

  private val Body =
    """{"meta":{"stream":"mediawiki.revision-create","domain":"en.wikipedia.org",""" +
      """"dt":"2026-11-01T10:00:00Z"},"database":"enwiki","page_id":42,"rev_id":1001}"""

  private def header(key: String, value: String): KafkaHeader =
    KafkaHeader(key, value.getBytes(UTF_8))

  private def record(
      body: Option[String] = Some(Body),
      offset: Long = 0L,
      headers: Option[Seq[KafkaHeader]] = Some(Seq(header(RawTopicNames.EventIdHeader, "[1]")))
  ): KafkaRecord =
    KafkaRecord(body.map(_.getBytes(UTF_8)), Topic, 2, offset, IngestedAt, headers)

  private def bronze(records: KafkaRecord*): Vector[BronzeRow] =
    BronzeEvents.fromKafka(records.toDF()).as[BronzeRow].collect().toVector.sortBy(_.kafka_offset)

  test("the columns are those of the table, in its order") {
    val columns = BronzeEvents.fromKafka(Seq(record()).toDF()).columns.toVector

    assert(columns == BronzeCols.All)
  }

  test("an event becomes one row that keeps the body and says where it came from") {
    val expected = BronzeRow(
      wiki = Some("enwiki"),
      stream = Some("mediawiki.revision-create"),
      event_id = Some("[1]"),
      event_ts = Instant.parse("2026-11-01T10:00:00Z"),
      ingested_at = IngestedAt,
      json = Body,
      kafka_topic = Topic,
      kafka_partition = 2,
      kafka_offset = 7L
    )

    assert(bronze(record(offset = 7L)) == Vector(expected))
  }

  test("the body is stored byte for byte, whatever is in it") {
    val accented = "caf\u00e9"
    val odd = s""" {"meta" : {"stream":"s","dt":"2026-11-01T10:00:00Z"}, "comment":"$accented"} """

    assert(bronze(record(body = Some(odd))).map(_.json) == Vector(odd))
  }

  test("an event time with a zone offset or fractions is read as the same moment") {
    def body(time: String): String = s"""{"meta":{"stream":"s","dt":"$time"},"database":"enwiki"}"""

    val rows = bronze(
      record(body = Some(body("2026-11-01T12:00:00.500+02:00")), offset = 1L),
      record(body = Some(body("2026-11-01T10:00:00.500Z")), offset = 2L)
    )

    assert(rows.map(_.event_ts).toSet == Set(Instant.parse("2026-11-01T10:00:00.500Z")))
  }

  test("an event without a readable time is filed under its ingest time") {
    val rows = bronze(
      record(body = Some("""{"meta":{"stream":"s"},"database":"enwiki"}"""), offset = 1L),
      record(body = Some("""{"meta":{"stream":"s","dt":"yesterday"},"database":"enwiki"}"""), 2L)
    )

    assert(rows.map(_.event_ts) == Vector(IngestedAt, IngestedAt))
    assert(rows.map(_.wiki) == Vector(Some("enwiki"), Some("enwiki")))
  }

  test("a body that is not an event still gets a row") {
    val rows = bronze(
      record(body = Some("not json at all"), offset = 1L),
      record(body = Some("[1, 2, 3]"), offset = 2L),
      record(body = Some(""), offset = 3L)
    )

    assert(rows.map(_.json) == Vector("not json at all", "[1, 2, 3]", ""))
    assert(rows.forall(row => row.wiki.isEmpty && row.stream.isEmpty))
    assert(rows.forall(_.event_ts == IngestedAt))
  }

  test("a record without the event id header has no event id") {
    val rows = bronze(
      record(headers = None, offset = 1L),
      record(headers = Some(Seq.empty), offset = 2L),
      record(headers = Some(Seq(header("something-else", "x"))), offset = 3L)
    )

    assert(rows.map(_.event_id) == Vector(None, None, None))
  }

  test("the event id is found among other headers, and the last one wins") {
    val headers = Seq(
      header(RawTopicNames.EventIdHeader, "[old]"),
      header("something-else", "x"),
      header(RawTopicNames.EventIdHeader, "[new]")
    )

    assert(bronze(record(headers = Some(headers))).map(_.event_id) == Vector(Some("[new]")))
  }

  test("a record without a value is left out") {
    assert(bronze(record(body = None, offset = 1L), record(offset = 2L)).map(_.kafka_offset) ==
      Vector(2L))
  }

  test("no records give no rows") {
    assert(bronze().isEmpty)
  }
}
