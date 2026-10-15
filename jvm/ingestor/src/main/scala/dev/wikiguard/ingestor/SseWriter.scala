package dev.wikiguard.ingestor

/**
 * Turns events back into the text of a server-sent event stream. The opposite of [[SseParser]].
 *
 * The output looks like what the live stream sends, so a recording written with this can be
 * replayed as if it had come over the wire.
 */
object SseWriter {

  /** One event as a block of lines, closed by an empty line. */
  def format(event: StreamEvent): String = {
    val id = event.id.map(value => s"id: $value")
    // A body that spans several lines needs one data line for each of them.
    val data = event.data.split("\n", -1).map(line => s"data: $line")
    (Vector("event: message") ++ id ++ data).mkString("", "\n", "\n\n")
  }
}
