package dev.wikiguard.ingestor

/** Where events come from: the live stream in production, a recording everywhere else. */
trait EventSource {

  /**
   * True for a source that goes on forever, so that a closed connection should be reopened. False
   * for a recording, which is finished once it has been read to the end.
   */
  def isEndless: Boolean

  /**
   * Opens one connection and hands its events to `consume`, in upstream order.
   *
   * The events are only valid inside `consume`; the connection is closed when it returns. The
   * result is whatever `consume` returned if the connection ended normally.
   *
   * @param resumeFrom
   *   id of the last event already handled; the source continues right after it
   */
  def connect[A](resumeFrom: Option[String])(
      consume: Iterator[StreamEvent] => A
  ): Either[SourceError, A]
}
