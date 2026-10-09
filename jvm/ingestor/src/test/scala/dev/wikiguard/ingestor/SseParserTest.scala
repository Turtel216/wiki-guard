package dev.wikiguard.ingestor

import org.scalatest.funsuite.AnyFunSuite

class SseParserTest extends AnyFunSuite {

  private def parse(text: String): Vector[StreamEvent] =
    SseParser.events(text.linesIterator).toVector

  test("an event has its id and data") {
    val events = parse("event: message\nid: [1]\ndata: {\"a\":1}\n\n")

    assert(events == Vector(StreamEvent(Some("[1]"), "{\"a\":1}")))
  }

  test("events are separated by empty lines") {
    val events = parse("id: 1\ndata: first\n\nid: 2\ndata: second\n\n")

    assert(events == Vector(StreamEvent(Some("1"), "first"), StreamEvent(Some("2"), "second")))
  }

  test("several data lines are joined with a newline") {
    assert(parse("data: one\ndata: two\n\n").map(_.data) == Vector("one\ntwo"))
  }

  test("an event without an id keeps the id of the one before") {
    val events = parse("id: 7\ndata: first\n\ndata: second\n\n")

    assert(events.map(_.id) == Vector(Some("7"), Some("7")))
  }

  test("comment lines and unknown fields are ignored") {
    val events = parse(": keep-alive\n\nretry: 1000\nid: 1\n:note\ndata: body\n\n")

    assert(events == Vector(StreamEvent(Some("1"), "body")))
  }

  test("a block without data is not an event but its id still counts") {
    val events = parse("id: 5\n\ndata: body\n\n")

    assert(events == Vector(StreamEvent(Some("5"), "body")))
  }

  test("only one space after the colon is removed") {
    assert(parse("data:  two spaces\n\ndata:none\n\n").map(_.data) == Vector(" two spaces", "none"))
  }

  test("a colon inside the value is kept") {
    assert(parse("data: {\"a\": \"b:c\"}\n\n").map(_.data) == Vector("{\"a\": \"b:c\"}"))
  }

  test("an event cut short by the end of the stream is dropped") {
    assert(parse("id: 1\ndata: whole\n\nid: 2\ndata: cut off").map(_.data) == Vector("whole"))
  }

  test("an empty stream has no events") {
    assert(parse("").isEmpty)
  }
}
