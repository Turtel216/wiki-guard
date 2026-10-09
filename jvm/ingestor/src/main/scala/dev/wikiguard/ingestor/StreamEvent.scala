package dev.wikiguard.ingestor

/**
 * One event as it came off the stream.
 *
 * The id is the stream's own position marker. Sending it back on reconnect makes the stream
 * continue right after this event. The data is the event body, kept exactly as received.
 */
final case class StreamEvent(id: Option[String], data: String)
