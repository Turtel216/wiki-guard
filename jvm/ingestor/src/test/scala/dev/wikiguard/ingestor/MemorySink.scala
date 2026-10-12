package dev.wikiguard.ingestor

import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

/**
 * A sink that keeps records in memory and resumes from the last one, like the real one.
 *
 * @param stored
 *   an event that was written before the test began
 * @param failOn
 *   event ids whose write fails
 */
final class MemorySink(stored: Option[ResumePoint] = None, failOn: Set[String] = Set.empty)
    extends RawEventSink {

  private val records = new ConcurrentLinkedQueue[RawRecord]()

  def written: Vector[RawRecord] = records.asScala.toVector

  def write(record: RawRecord): Either[SinkError, Unit] =
    if (record.eventId.exists(failOn.contains)) {
      Left(SinkError.WriteFailed(record.topic, "broker is down"))
    } else {
      records.add(record)
      Right(())
    }

  def lastEvent(): Either[SinkError, Option[ResumePoint]] = {
    val latest = for {
      record <- written.lastOption
      id <- record.eventId
    } yield ResumePoint(id, record.eventTime.getOrElse(Instant.EPOCH))
    Right(latest.orElse(stored))
  }
}
