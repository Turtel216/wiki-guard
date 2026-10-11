package dev.wikiguard.ingestor

import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

class FixtureSourceTest extends AnyFunSuite {

  private val events = Vector(
    SampleEvents.event("a", SampleEvents.body(time = "2026-11-01T10:00:00Z")),
    SampleEvents.event("b", SampleEvents.body(time = "2026-11-01T10:00:10Z")),
    SampleEvents.event("c", SampleEvents.body(time = "2026-11-01T10:00:40Z"))
  )

  private def recording(content: Vector[StreamEvent] = events): Path = {
    val file = Files.createTempFile("fixture", ".sse")
    file.toFile.deleteOnExit()
    Files.writeString(file, SampleEvents.wire(content))
  }

  private def source(file: Path, speed: Double = 1000.0): FixtureSource =
    new FixtureSource(file, speed, _ => ())

  test("every event in the file is replayed in order") {
    val replayed = source(recording()).connect(None)(_.toVector)

    assert(replayed == Right(events))
  }

  test("a recording is not endless") {
    assert(!source(recording()).isEndless)
  }

  test("replay continues after the event it is resumed from") {
    val replayed = source(recording()).connect(Some("a"))(_.toVector)

    assert(replayed == Right(events.drop(1)))
  }

  test("resuming from the last event replays nothing") {
    assert(source(recording()).connect(Some("c"))(_.toVector) == Right(Vector.empty))
  }

  test("an id that is not in the file starts the replay from the top") {
    val replayed = source(recording()).connect(Some("from-elsewhere"))(_.toVector)

    assert(replayed == Right(events))
  }

  test("waits between events follow their timestamps, divided by the speed") {
    val waits = new ConcurrentLinkedQueue[FiniteDuration]()
    val paced = new FixtureSource(recording(), speed = 10.0, wait => { waits.add(wait); () })

    paced.connect(None)(_.size)

    assert(waits.asScala.toVector == Vector(1.second, 3.seconds))
  }

  test("events that go back in time or have no time cause no wait") {
    val waits = new ConcurrentLinkedQueue[FiniteDuration]()
    val unordered = Vector(
      SampleEvents.event("a", SampleEvents.body(time = "2026-11-01T10:00:10Z")),
      SampleEvents.event("b", SampleEvents.body(time = "2026-11-01T10:00:00Z")),
      SampleEvents.event("c", "not json")
    )
    val paced = new FixtureSource(recording(unordered), 1.0, wait => { waits.add(wait); () })

    val replayed = paced.connect(None)(_.toVector)

    assert(replayed == Right(unordered))
    assert(waits.asScala.forall(_ == 0.seconds))
  }

  test("a missing file is reported as unreachable") {
    val result = source(Path.of("no-such-recording.sse")).connect(None)(_.size)

    assert(result.left.exists(_.isInstanceOf[SourceError.Unreachable]))
  }
}
