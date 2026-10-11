package dev.wikiguard.ingestor

/** Keeps a record of lost stretches, so that labels overlapping them can be distrusted. */
trait GapRecorder {

  /** May be called more than once for the same gap if the ingestor restarts at a bad moment. */
  def record(gap: IngestGap): Unit
}
