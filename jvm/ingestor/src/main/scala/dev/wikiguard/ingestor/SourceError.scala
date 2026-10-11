package dev.wikiguard.ingestor

import scala.concurrent.duration.FiniteDuration

/** Why a connection to an event source did not run to a normal end. */
sealed trait SourceError

object SourceError {

  /** No connection could be opened. */
  final case class Unreachable(reason: String) extends SourceError

  /** The server answered, but not with a stream. It may say how long to stay away. */
  final case class Rejected(status: Int, retryAfter: Option[FiniteDuration]) extends SourceError {

    /** Too many requests, or a server asking clients to back off. */
    def isThrottled: Boolean = status == 429 || status == 503
  }

  /** The connection broke while events were flowing. */
  final case class Interrupted(reason: String) extends SourceError
}
