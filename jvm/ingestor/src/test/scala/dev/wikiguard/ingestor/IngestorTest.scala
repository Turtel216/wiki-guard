package dev.wikiguard.ingestor

import java.time.{Clock, Instant, ZoneOffset}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

import io.prometheus.metrics.model.registry.PrometheusRegistry
import org.scalatest.funsuite.AnyFunSuite

import dev.wikiguard.ingestor.ScriptedSource.{broken, closed}

class IngestorTest extends AnyFunSuite {

  private val Now = Instant.parse("2026-11-10T12:00:00Z")

  private val first = SampleEvents.event("[1]", SampleEvents.body(pageId = 1L))
  private val second = SampleEvents.event("[2]", SampleEvents.body(pageId = 2L))
  private val third = SampleEvents.event("[3]", SampleEvents.body(pageId = 3L))
  private val canary = SampleEvents.event("[c]", SampleEvents.body(domain = "canary"))
  private val german = SampleEvents.event("[d]", SampleEvents.body(wiki = "dewiki"))

  /** One run of the ingestor, with everything it touched kept for inspection. */
  final private class Run(
      source: EventSource,
      val sink: MemorySink,
      gapsFail: Boolean = false
  ) {
    private val sleeps = new ConcurrentLinkedQueue[FiniteDuration]()
    private val foundGaps = new ConcurrentLinkedQueue[IngestGap]()

    private val timing = Ingestor.Timing(
      backoff = Backoff(1.second, 60.seconds),
      upstreamRetention = 7.days,
      clock = Clock.fixed(Now, ZoneOffset.UTC),
      sleep = wait => { sleeps.add(wait); () },
      jitter = () => 1.0
    )

    val result: Either[IngestError, Unit] = new Ingestor(
      source,
      sink,
      new EventRouter(Set("enwiki"), RawTopics.ByStream),
      gap => {
        foundGaps.add(gap)
        Either.cond(!gapsFail, (), SinkError.WriteFailed("gaps", "broker is down"))
      },
      new IngestMetrics(new PrometheusRegistry),
      timing
    ).run()

    def waits: Vector[FiniteDuration] = sleeps.asScala.toVector
    def gaps: Vector[IngestGap] = foundGaps.asScala.toVector
    def writtenIds: Vector[String] = sink.written.flatMap(_.eventId)
  }

  private def run(script: ScriptedSource.Connection*): Run = runInto(new MemorySink)(script: _*)

  private def runInto(sink: MemorySink)(script: ScriptedSource.Connection*): Run =
    new Run(new ScriptedSource(script), sink)

  test("kept events are written in order and the rest are dropped") {
    val done = run(closed(first, canary, german, second))

    assert(done.result == Right(()))
    assert(done.writtenIds == Vector("[1]", "[2]"))
  }

  test("a closed connection is reopened at once, right after the last written event") {
    val source = new ScriptedSource(Seq(closed(first, second), closed(third), closed()))

    val done = new Run(source, new MemorySink)

    assert(done.writtenIds == Vector("[1]", "[2]", "[3]"))
    assert(source.resumedFrom == Vector(None, Some("[2]"), Some("[3]")))
    assert(done.waits.isEmpty)
  }

  test("a restart resumes from what an earlier run left in the sink") {
    val source = new ScriptedSource(Seq(closed(second)))
    val sink = new MemorySink(stored = Some(ResumePoint("[1]", Now.minusSeconds(60))))

    new Run(source, sink)

    assert(source.resumedFrom == Vector(Some("[1]")))
  }

  test("a connection that breaks after delivering events is reopened at once") {
    val done = run(broken(SourceError.Interrupted("reset"), first), closed(second))

    assert(done.writtenIds == Vector("[1]", "[2]"))
    assert(done.waits.isEmpty)
  }

  test("failures in a row wait longer each time") {
    val down = broken(SourceError.Unreachable("no route"))

    val done = run(down, down, down, closed(first))

    assert(done.waits == Vector(1.second, 2.seconds, 4.seconds))
    assert(done.writtenIds == Vector("[1]"))
  }

  test("a connection that closes without a single event counts as a failure") {
    val done = run(closed(), closed(), closed(first), closed())

    assert(done.waits.take(2) == Vector(1.second, 2.seconds))
  }

  test("a working connection resets the wait") {
    val down = broken(SourceError.Unreachable("no route"))

    val done = run(down, down, closed(first), down, closed(second))

    assert(done.waits == Vector(1.second, 2.seconds, 1.second))
  }

  test("a throttled client waits as long as it is told to") {
    val done = run(broken(SourceError.Rejected(429, Some(300.seconds))), closed(first))

    assert(done.waits == Vector(300.seconds))
  }

  test("a throttled client that is told nothing waits the longest wait") {
    val done = run(broken(SourceError.Rejected(503, None)), closed(first))

    assert(done.waits == Vector(60.seconds))
  }

  test("a short wait from the server never shortens the usual one") {
    val rejected = broken(SourceError.Rejected(429, Some(1.second)))

    val done = run(rejected, rejected, rejected, closed(first))

    assert(done.waits == Vector(1.second, 2.seconds, 4.seconds))
  }

  test("a failed write stops the ingestor and nothing after it is written") {
    val sink = new MemorySink(failOn = Set("[2]"))

    val done = runInto(sink)(closed(first, second, third), closed())

    assert(done.result.left.exists(_.message.contains("broker is down")))
    assert(done.writtenIds == Vector("[1]"))
  }

  test("a recording that cannot be read stops the ingestor") {
    val missing = new FixtureSource(java.nio.file.Path.of("no-such-file.sse"), 1.0, _ => ())

    val done = new Run(missing, new MemorySink)

    assert(done.result.left.exists(_.isInstanceOf[IngestError.ReplayFailed]))
    assert(done.waits.isEmpty)
  }

  test("resuming from an event older than the source keeps is reported as a gap") {
    val lastStored = Now.minusSeconds(8 * 24 * 3600)
    val sink = new MemorySink(stored = Some(ResumePoint("[old]", lastStored)))
    val arrives = SampleEvents.event("[1]", SampleEvents.body(time = "2026-11-03T12:00:05Z"))

    val done = runInto(sink)(closed(arrives))

    assert(done.gaps == Vector(IngestGap(lastStored, Instant.parse("2026-11-03T12:00:05Z"))))
    assert(done.writtenIds == Vector("[1]"))
  }

  test("a gap ends at the first event even when that event is dropped") {
    val lastStored = Now.minusSeconds(8 * 24 * 3600)
    val sink = new MemorySink(stored = Some(ResumePoint("[old]", lastStored)))
    val other =
      SampleEvents.event("[d]", SampleEvents.body(wiki = "dewiki", time = "2026-11-03T12:00:01Z"))

    val done = runInto(sink)(closed(other, first))

    assert(done.gaps.map(_.to) == Vector(Instant.parse("2026-11-03T12:00:01Z")))
  }

  test("a gap that cannot be recorded stops the ingestor before anything is written") {
    val stale = ResumePoint("[old]", Now.minusSeconds(8 * 24 * 3600))
    val source = new ScriptedSource(Seq(closed(first, second), closed()))

    val done = new Run(source, new MemorySink(stored = Some(stale)), gapsFail = true)

    assert(done.result.left.exists(_.message.contains("broker is down")))
    assert(done.writtenIds.isEmpty)
  }

  test("a recent resume point is not a gap") {
    val sink = new MemorySink(stored = Some(ResumePoint("[old]", Now.minusSeconds(6 * 24 * 3600))))

    val done = runInto(sink)(closed(first))

    assert(done.gaps.isEmpty)
  }

  test("no gap is reported while an old resume point gets no events") {
    val sink = new MemorySink(stored = Some(ResumePoint("[old]", Now.minusSeconds(30 * 24 * 3600))))

    val done = runInto(sink)(broken(SourceError.Unreachable("no route")), closed())

    assert(done.gaps.isEmpty)
    assert(done.waits.headOption.exists(_ > Duration.Zero))
  }
}
