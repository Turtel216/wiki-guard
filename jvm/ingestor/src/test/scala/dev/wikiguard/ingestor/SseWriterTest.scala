package dev.wikiguard.ingestor

import org.scalatest.funsuite.AnyFunSuite

class SseWriterTest extends AnyFunSuite {

  private def parse(text: String): Vector[StreamEvent] =
    SseParser.events(text.linesIterator).toVector

  test("an event is written the way the live stream sends it") {
    val text = SseWriter.format(StreamEvent(Some("[1]"), """{"rev_id":1}"""))

    assert(text == "event: message\nid: [1]\ndata: {\"rev_id\":1}\n\n")
  }

  test("an event without an id has no id line") {
    val text = SseWriter.format(StreamEvent(None, "{}"))

    assert(text == "event: message\ndata: {}\n\n")
  }

  test("written events parse back to what they were") {
    val events = Vector(
      SampleEvents.event("a"),
      SampleEvents.event("b", SampleEvents.body(pageId = 7L)),
      SampleEvents.event("c", "two\nlines")
    )

    assert(parse(events.map(SseWriter.format).mkString) == events)
  }
}
