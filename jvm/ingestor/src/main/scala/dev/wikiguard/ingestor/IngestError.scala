package dev.wikiguard.ingestor

/** A failure the ingestor cannot recover from by reconnecting. */
sealed trait IngestError {
  def message: String
}

object IngestError {

  /** Kafka is not taking events or not telling where to resume. */
  final case class SinkFailed(error: SinkError) extends IngestError {
    def message: String = error.message
  }

  /** A recording could not be replayed. The live stream never ends up here; it is retried. */
  final case class ReplayFailed(error: SourceError) extends IngestError {
    def message: String = s"could not replay the recording: $error"
  }
}
