package dev.wikiguard.ingestor

/** Why an event was not written to Kafka. The label is used in metrics. */
sealed trait DropReason {
  def label: String
}

object DropReason {

  /** A synthetic event upstream sends to test its own pipeline. */
  case object Canary extends DropReason {
    val label: String = "canary"
  }

  /** A real event for a wiki we do not follow. */
  case object OtherWiki extends DropReason {
    val label: String = "other_wiki"
  }

  /** An event from a stream that has no raw topic. */
  case object UnknownStream extends DropReason {
    val label: String = "unknown_stream"
  }

  /** The body is not JSON, or lacks the stream name or the wiki. */
  case object Malformed extends DropReason {
    val label: String = "malformed"
  }
}
