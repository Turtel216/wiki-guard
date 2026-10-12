package dev.wikiguard.ingestor

import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

/**
 * A source that plays out a fixed list of connections, one per call. It claims to be endless until
 * the list runs out, which is how a test brings the ingestor to a stop.
 */
final class ScriptedSource(script: Seq[ScriptedSource.Connection]) extends EventSource {

  private val remaining = new ConcurrentLinkedQueue(script.asJava)
  private val resumes = new ConcurrentLinkedQueue[Option[String]]()

  /** The resume point each connection was opened with, in order. */
  def resumedFrom: Vector[Option[String]] = resumes.asScala.toVector

  def isEndless: Boolean = !remaining.isEmpty

  def connect[A](resumeFrom: Option[String])(
      consume: Iterator[StreamEvent] => A
  ): Either[SourceError, A] = {
    resumes.add(resumeFrom)
    Option(remaining.poll()).getOrElse(ScriptedSource.Connection(Vector.empty, None)) match {
      case ScriptedSource.Connection(events, None) => Right(consume(events.iterator))
      case ScriptedSource.Connection(events, Some(error)) =>
        consume(events.iterator)
        Left(error)
    }
  }
}

object ScriptedSource {

  /** Events that are delivered, then either a normal close or the given error. */
  final case class Connection(events: Vector[StreamEvent], error: Option[SourceError])

  def closed(events: StreamEvent*): Connection = Connection(events.toVector, None)

  def broken(error: SourceError, events: StreamEvent*): Connection =
    Connection(events.toVector, Some(error))
}
