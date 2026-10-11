package dev.wikiguard.ingestor

import java.time.Instant

import org.scalatest.funsuite.AnyFunSuite

class EventRouterTest extends AnyFunSuite {

  private val router = new EventRouter(Set("enwiki"), RawTopics.ByStream)

  private def route(body: String): Either[DropReason, RawRecord] =
    router.route(StreamEvent(Some("id-1"), body))

  test("an edit on a followed wiki goes to the revision topic with the body untouched") {
    val body = SampleEvents.body(pageId = 7L, time = "2026-11-01T10:00:00Z")

    val expected = RawRecord(
      topic = "wg.raw.revision-create",
      pageId = Some(7L),
      eventId = Some("id-1"),
      eventTime = Some(Instant.parse("2026-11-01T10:00:00Z")),
      json = body
    )
    assert(route(body) == Right(expected))
  }

  test("a tag change goes to its own topic") {
    val record = route(SampleEvents.body(stream = SampleEvents.TagsChange))

    assert(record.map(_.topic) == Right("wg.raw.revision-tags-change"))
  }

  test("canary events are dropped even when they name a followed wiki") {
    assert(route(SampleEvents.body(domain = "canary")) == Left(DropReason.Canary))
  }

  test("events for other wikis are dropped") {
    assert(route(SampleEvents.body(wiki = "dewiki")) == Left(DropReason.OtherWiki))
  }

  test("events from a stream without a topic are dropped") {
    val body = SampleEvents.body(stream = "mediawiki.page-delete")

    assert(route(body) == Left(DropReason.UnknownStream))
  }

  test("a body that is not JSON is dropped as malformed") {
    assert(route("not json") == Left(DropReason.Malformed))
    assert(route("") == Left(DropReason.Malformed))
    assert(route("[1, 2]") == Left(DropReason.Malformed))
  }

  test("a body without a stream name or a wiki is dropped as malformed") {
    assert(route("""{"database":"enwiki","page_id":1}""") == Left(DropReason.Malformed))
    assert(route("""{"meta":{"stream":"mediawiki.revision-create"}}""") ==
      Left(DropReason.Malformed))
  }

  test("a missing page id or time does not stop an event") {
    val body = """{"meta":{"stream":"mediawiki.revision-create"},"database":"enwiki"}"""

    val record = route(body)

    assert(record.map(_.pageId) == Right(None))
    assert(record.map(_.eventTime) == Right(None))
  }

  test("a time that cannot be read is treated as missing") {
    val record = route(SampleEvents.body(time = "yesterday"))

    assert(record.map(_.eventTime) == Right(None))
  }
}
