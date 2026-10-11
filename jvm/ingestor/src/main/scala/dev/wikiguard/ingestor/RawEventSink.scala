package dev.wikiguard.ingestor

/** Where kept events are stored. It also remembers how far the ingestor got. */
trait RawEventSink {

  /** Stores one event. It returns only once the event is safely stored, or has failed. */
  def write(record: RawRecord): Either[SinkError, Unit]

  /** The newest stored event that carries a stream position, if there is one. */
  def lastEvent(): Either[SinkError, Option[ResumePoint]]
}
