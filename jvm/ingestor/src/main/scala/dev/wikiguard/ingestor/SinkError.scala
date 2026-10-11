package dev.wikiguard.ingestor

/** Why the place events are written to could not do its job. */
sealed trait SinkError {
  def message: String
}

object SinkError {

  final case class WriteFailed(topic: String, reason: String) extends SinkError {
    def message: String = s"could not write to $topic: $reason"
  }

  final case class ReadFailed(reason: String) extends SinkError {
    def message: String = s"could not read the last written event: $reason"
  }
}
