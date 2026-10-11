package dev.wikiguard.ingestor

import java.time.Instant

/**
 * A stretch of time whose events were lost for good.
 *
 * @param from
 *   time of the last event stored before the gap
 * @param to
 *   time of the first event received after it
 */
final case class IngestGap(from: Instant, to: Instant)
