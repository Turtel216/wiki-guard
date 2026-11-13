package dev.wikiguard.ingestor

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Instant

import scala.util.{Try, Using}

/**
 * Saves the newest events from the raw topics as a file that [[FixtureSource]] can replay.
 *
 * The events come from Kafka, where the ingestor put them, and never from the stream itself.
 */
final class FixtureRecorder(reader: RawTopicReader) {

  import FixtureRecorder.Summary

  /** Records up to `maxEvents` events into `file`, replacing it if it is already there. */
  def record(maxEvents: Int, file: Path): Either[RecordingError, Summary] =
    for {
      events <- reader.newest(maxEvents)
      _ <- Either.cond(events.nonEmpty, (), RecordingError.NothingToRecord)
      summary = summarize(events)
      _ <- write(file, summary, events)
    } yield summary

  private def summarize(events: Vector[StreamEvent]): Summary = {
    val times = events.flatMap(event => EventEnvelope.parse(event.data).flatMap(_.time))
    Summary(events.size, times.minOption, times.maxOption)
  }

  private def write(
      file: Path,
      summary: Summary,
      events: Vector[StreamEvent]
  ): Either[RecordingError, Unit] =
    Try {
      Option(file.toAbsolutePath.getParent).foreach(parent => Files.createDirectories(parent))
      Using.resource(Files.newBufferedWriter(file, StandardCharsets.UTF_8)) { writer =>
        writer.write(FixtureRecorder.header(summary))
        events.foreach(event => writer.write(SseWriter.format(event)))
      }
    }.toEither.left.map(error => RecordingError.WriteFailed(file, error.toString))
}

object FixtureRecorder {

  /**
   * What ended up in a recording.
   *
   * @param from
   *   the time of the earliest event, if any event carries a time
   * @param to
   *   the time of the latest event
   */
  final case class Summary(events: Int, from: Option[Instant], to: Option[Instant])

  /** Comment lines at the top of the file. Anything that reads the stream format skips them. */
  private def header(summary: Summary): String = {
    val span = for {
      from <- summary.from
      to <- summary.to
    } yield s" from $from to $to"
    s": Recorded from the raw Kafka topics: ${summary.events} events${span.getOrElse("")}.\n" +
      ": Canaries and other wikis are missing, because the ingestor never stores them.\n\n"
  }
}
