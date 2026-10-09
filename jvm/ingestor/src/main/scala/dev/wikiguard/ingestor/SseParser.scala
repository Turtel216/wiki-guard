package dev.wikiguard.ingestor

import scala.annotation.tailrec

/**
 * Turns the lines of a server-sent event stream into events.
 *
 * An event is a block of "field: value" lines closed by an empty line. Only the id and data fields
 * matter here. As the format requires, an id stays in effect for later events until another one
 * replaces it, and a block without data is not an event.
 */
object SseParser {

  def events(lines: Iterator[String]): Iterator[StreamEvent] =
    Iterator
      .unfold(Option.empty[String]) { lastId =>
        readBlock(lines, Vector.empty).map { block =>
          val id = block.collect { case Field("id", value) => value }.lastOption.orElse(lastId)
          val data = block.collect { case Field("data", value) => value }
          val event = if (data.isEmpty) None else Some(StreamEvent(id, data.mkString("\n")))
          (event, id)
        }
      }
      .flatten

  /** Reads up to the next empty line. A block cut short by the end of the stream is dropped. */
  @tailrec
  private def readBlock(lines: Iterator[String], block: Vector[String]): Option[Vector[String]] =
    if (!lines.hasNext) {
      None
    } else {
      val line = lines.next()
      if (line.isEmpty) Some(block) else readBlock(lines, block :+ line)
    }

  private object Field {

    /** Splits "name: value". Lines starting with a colon are comments and match nothing. */
    def unapply(line: String): Option[(String, String)] =
      line.indexOf(':') match {
        case 0 => None
        case -1 => Some((line, ""))
        case colon => Some((line.take(colon), line.drop(colon + 1).stripPrefix(" ")))
      }
  }
}
