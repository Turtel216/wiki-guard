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

  /** The recording could not be saved. */
  final case class WriteFailed(file: Path, reason: String) extends RecordingError {
    def message: String = s"could not write $file: $reason"
  }
}
