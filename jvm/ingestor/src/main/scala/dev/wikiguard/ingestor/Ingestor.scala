package dev.wikiguard.ingestor

import java.time.{Clock, Duration => JavaDuration}
import java.util.concurrent.atomic.LongAdder

import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.jdk.DurationConverters._

import org.slf4j.LoggerFactory

/**
 * Copies events from a source into the raw topics, reconnecting for as long as the source lasts.
 *
 * Nothing is remembered between connections. Before each one the sink is asked for the newest
 * stored event, and the source continues right after it. A crash at any moment therefore costs at
 * most a few repeated events, never a lost one.
 */
final class Ingestor(
    source: EventSource,
    sink: RawEventSink,
    router: EventRouter,
    gaps: GapRecorder,
    metrics: IngestMetrics,
    timing: Ingestor.Timing
) {

  import Ingestor.Outcome

  private val logger = LoggerFactory.getLogger(getClass)

  /** Returns normally only when a recording has been replayed to its end. */
  def run(): Either[IngestError, Unit] = loop(failures = 0)

  @tailrec
  private def loop(failures: Int): Either[IngestError, Unit] =
    connectOnce() match {
      case Outcome.Finished => Right(())
      case Outcome.Fatal(error) => Left(error)
      case Outcome.Reconnect => loop(failures = 0)
      case Outcome.Retry(atLeast) =>
        val wait = timing.backoff.delay(failures + 1, timing.jitter()).max(atLeast)
        logger.warn("Reconnecting in {} after {} failed attempts", wait, failures + 1)
        timing.sleep(wait)
        loop(failures + 1)
    }

  private def connectOnce(): Outcome =
    sink.lastEvent() match {
      case Left(error) => Outcome.Fatal(IngestError.SinkFailed(error))
      case Right(resume) =>
        logger.info("Connecting, resuming after {}", resume.fold("nothing")(_.eventTime.toString))
        val delivered = new LongAdder
        val result = source.connect(resume.map(_.eventId)) { events =>
          noteGap(resume, events)
            .map { event =>
              delivered.increment()
              handle(event)
            }
            .collectFirst { case Left(error) => error }
        }
        val outcome = judge(result, delivered.sum > 0)
        logger.info("Connection ended after {} events: {}", delivered.sum, outcome)
        metrics.connectionEnded(outcome.label)
        outcome
    }

  /** Filters one event and stores it if it is kept. Only a failed write is an error. */
  private def handle(event: StreamEvent): Either[SinkError, Unit] = {
    metrics.eventReceived()
    router.route(event) match {
      case Left(reason) =>
        metrics.eventDropped(reason)
        Right(())
      case Right(record) =>
        sink.write(record).map { _ =>
          metrics.eventWritten(record.topic, timing.clock.instant(), record.eventTime)
        }
    }
  }

  /**
   * Decides what to do after a connection has ended.
   *
   * A connection that delivered events was a working one, however it ended, so the next one is
   * opened at once. Anything else counts as a failure and is retried with a growing wait.
   */
  private def judge(result: Either[SourceError, Option[SinkError]], hadEvents: Boolean): Outcome =
    result match {
      case Right(Some(error)) => Outcome.Fatal(IngestError.SinkFailed(error))
      case Right(None) if !source.isEndless => Outcome.Finished
      case Left(error) if !source.isEndless => Outcome.Fatal(IngestError.ReplayFailed(error))
      case Left(error: SourceError.Rejected) => Outcome.Retry(stayAway(error))
      case Right(None) | Left(_: SourceError.Interrupted) if hadEvents => Outcome.Reconnect
      case Right(None) | Left(_: SourceError.Interrupted | _: SourceError.Unreachable) =>
        Outcome.Retry(Duration.Zero)
    }

  /** A throttled client waits as long as it is told to, or the longest wait if not told. */
  private def stayAway(error: SourceError.Rejected): FiniteDuration =
    error.retryAfter.getOrElse(if (error.isThrottled) timing.backoff.max else Duration.Zero)

  /**
   * Reports a gap when the resume point is older than what the source still has. The gap ends at
   * the first event that arrives, so nothing is reported for a connection that delivers nothing.
   */
  private def noteGap(
      resume: Option[ResumePoint],
      events: Iterator[StreamEvent]
  ): Iterator[StreamEvent] =
    resume.filter(isTooOld).fold(events) { point =>
      val buffered = events.buffered
      buffered.headOption.foreach { first =>
        val firstTime = EventEnvelope.parse(first.data).flatMap(_.time)
        gaps.record(IngestGap(point.eventTime, firstTime.getOrElse(timing.clock.instant())))
        metrics.gapFound()
      }
      buffered
    }

  private def isTooOld(point: ResumePoint): Boolean = {
    val age = JavaDuration.between(point.eventTime, timing.clock.instant())
    age.compareTo(timing.upstreamRetention.toJava) > 0
  }
}

object Ingestor {

  /**
   * Everything about time, so that tests can run without waiting.
   *
   * @param upstreamRetention
   *   how far back the source lets a client resume
   * @param sleep
   *   waits for the given time
   * @param jitter
   *   gives a random number from 0 to 1
   */
  final case class Timing(
      backoff: Backoff,
      upstreamRetention: FiniteDuration,
      clock: Clock,
      sleep: FiniteDuration => Unit,
      jitter: () => Double
  )

  /** What to do after one connection has ended. The label is used in metrics. */
  sealed private trait Outcome {
    def label: String
  }

  private object Outcome {

    /** A recording was replayed to its end. */
    case object Finished extends Outcome {
      val label: String = "finished"
    }

    /** The connection worked and ended; open the next one at once. */
    case object Reconnect extends Outcome {
      val label: String = "closed"
    }

    /** The connection did not work; wait, and for no less than the given time. */
    final case class Retry(atLeast: FiniteDuration) extends Outcome {
      val label: String = "failed"
    }

    final case class Fatal(error: IngestError) extends Outcome {
      val label: String = "fatal"
    }
  }
}
