package dev.wikiguard.ingestor

import java.time.Instant

/** An event that passed the filters, ready to be written to its raw topic. */
final case class RawRecord(
    topic: String,
    pageId: Option[Long],
    eventId: Option[String],
    eventTime: Option[Instant],
    json: String
)
