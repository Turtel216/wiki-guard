package dev.wikiguard.ingestor

import org.slf4j.LoggerFactory

/**
 * Writes gaps to the log and nowhere else.
 *
 * This is a placeholder until gaps have a table to go to.
 */
final class LoggingGapRecorder extends GapRecorder {

  private val logger = LoggerFactory.getLogger(getClass)

  def record(gap: IngestGap): Unit =
    logger.error("Events between {} and {} are lost for good", gap.from, gap.to)
}
