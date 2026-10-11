package dev.wikiguard.ingestor

import java.time.Instant

/**
 * The newest event that is safely stored.
 *
 * @param eventId
 *   the stream position to continue after
 * @param eventTime
 *   when the event happened upstream, used to tell whether the stream still reaches back that far
 */
final case class ResumePoint(eventId: String, eventTime: Instant)
