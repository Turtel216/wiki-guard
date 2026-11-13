package dev.wikiguard.ingestor

import java.nio.file.Path

/** Why a recording could not be made. */
sealed trait RecordingError {
  def message: String
}

object RecordingError {

  /** The events could not be read back from Kafka. */
  final case class ReadFailed(reason: String) extends RecordingError {
    def message: String = s"could not read the raw topics: $reason"
  }

  /** The raw topics hold no events yet. */
  case object NothingToRecord extends RecordingError {
    val message: String = "the raw topics are empty, so there is nothing to record"
  }

  /** The recording could not be saved. */
  final case class WriteFailed(file: Path, reason: String) extends RecordingError {
    def message: String = s"could not write $file: $reason"
  }
}
