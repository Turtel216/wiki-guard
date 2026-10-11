package dev.wikiguard.ingestor

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.{Duration, Instant}

import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

/**
 * Replays a recording of the stream from a file. Used wherever the real stream must not be
 * contacted.
 *
 * The file holds the stream exactly as it came over the wire. Events are spaced out by the gaps
 * between their own timestamps, divided by the speed.
 *
 * @param speed
 *   how many times faster than real life to replay
 * @param sleep
 *   waits for the given time
 */
final class FixtureSource(file: Path, speed: Double, sleep: FiniteDuration => Unit)
    extends EventSource {
  require(speed > 0, "speed must be above zero")

  val isEndless: Boolean = false

  def connect[A](resumeFrom: Option[String])(
      consume: Iterator[StreamEvent] => A
  ): Either[SourceError, A] =
    Try {
      val start = resumeFrom.filter(isInFile)
      withEvents(events => consume(paced(after(start, events))))
    }.toEither.left.map(error => SourceError.Unreachable(error.toString))

  private def withEvents[A](use: Iterator[StreamEvent] => A): A =
    Using.resource(Files.newBufferedReader(file, StandardCharsets.UTF_8)) { reader =>
      use(SseParser.events(reader.lines.iterator.asScala))
    }

  /** An id from some other recording is ignored and the file is replayed from the top. */
  private def isInFile(id: String): Boolean =
    withEvents(events => events.exists(_.id.contains(id)))

  private def after(id: Option[String], events: Iterator[StreamEvent]) =
    id.fold(events)(last => events.dropWhile(!_.id.contains(last)).drop(1))

  private def paced(events: Iterator[StreamEvent]): Iterator[StreamEvent] =
    events
      .scanLeft(FixtureSource.Step(None, None)) { (previous, event) =>
        val time = EventEnvelope.parse(event.data).flatMap(_.time).orElse(previous.time)
        for {
          before <- previous.time
          now <- time
        } sleep(gap(before, now))
        FixtureSource.Step(Some(event), time)
      }
      .flatMap(_.event)

  private def gap(before: Instant, now: Instant): FiniteDuration = {
    val nanos = Duration.between(before, now).toNanos
    (math.max(nanos, 0L) / speed).toLong.nanos
  }
}

object FixtureSource {

  /** An event about to be handed out, with the time of the latest event that had one. */
  final private case class Step(event: Option[StreamEvent], time: Option[Instant])
}
