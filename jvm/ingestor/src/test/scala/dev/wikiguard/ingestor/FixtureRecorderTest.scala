package dev.wikiguard.ingestor

import java.nio.file.{Files, Path}
import java.time.{Duration, Instant}

import org.scalatest.funsuite.AnyFunSuite

import dev.wikiguard.ingestor.MockRawTopics.Stored

class FixtureRecorderTest extends AnyFunSuite {

  private val Revisions = "wg.raw.revision-create"
  private val Tags = "wg.raw.revision-tags-change"
  private val Topics = Set(Revisions, Tags)

  /** Five events, one a second, written to alternating topics. */
  private val events: Vector[StreamEvent] = (1 to 5).toVector.map { number =>
    val stream = if (number % 2 == 0) SampleEvents.TagsChange else SampleEvents.RevisionCreate
    val time = Instant.parse("2026-11-01T10:00:00Z").plusSeconds(number.toLong)
    SampleEvents.event(s"[$number]", SampleEvents.body(stream = stream, time = time.toString))
  }

  private val stored: Vector[Stored] = events.zipWithIndex.map { case (event, index) =>
    val topic = if (event.data.contains(SampleEvents.TagsChange)) Tags else Revisions
    Stored(topic, 0, offset = (index / 2).toLong, writtenAt = 1000L + index, event.id, event.data)
  }

  private def recorder(records: Seq[Stored] = stored): FixtureRecorder =
    new FixtureRecorder(
      new RawTopicReader(
        () => MockRawTopics.consumer(Topics, records),
        Topics,
        Duration.ofMillis(200)
      )
    )

  private def newFile(): Path = {
    val directory = Files.createTempDirectory("recorder")
    directory.toFile.deleteOnExit()
    directory.resolve("recording.sse")
  }

  private def replay(file: Path): Either[SourceError, Vector[StreamEvent]] =
    new FixtureSource(file, speed = 1.0, sleep = _ => ()).connect(None)(_.toVector)

  test("a recording replays as the events that were stored, in order") {
    val file = newFile()

    assert(recorder().record(maxEvents = 100, file).isRight)

    assert(replay(file) == Right(events))
  }

  test("the summary counts the events and gives the time they span") {
    val summary = recorder().record(maxEvents = 3, newFile())

    val expected = FixtureRecorder.Summary(
      events = 3,
      from = Some(Instant.parse("2026-11-01T10:00:03Z")),
      to = Some(Instant.parse("2026-11-01T10:00:05Z"))
    )
    assert(summary == Right(expected))
  }

  test("the file starts with a comment that says what it is") {
    val file = newFile()

    recorder().record(maxEvents = 100, file)

    val firstLine = Files.readAllLines(file).get(0)
    assert(firstLine.startsWith(": Recorded from the raw Kafka topics: 5 events"))
  }

  test("an existing file is replaced") {
    val file = newFile()
    Files.writeString(file, SampleEvents.wire(Seq(SampleEvents.event("old"))))

    assert(recorder().record(maxEvents = 2, file).isRight)

    assert(replay(file) == Right(events.takeRight(2)))
  }

  test("missing folders on the way to the file are created") {
    val file = newFile().resolveSibling("deeper").resolve("recording.sse")

    assert(recorder().record(maxEvents = 100, file).isRight)

    assert(Files.exists(file))
  }

  test("empty topics are an error and leave no file behind") {
    val file = newFile()

    assert(recorder(Nil).record(maxEvents = 100, file) == Left(RecordingError.NothingToRecord))

    assert(!Files.exists(file))
  }

  test("a file that cannot be written is reported") {
    val folder = Files.createTempDirectory("not-a-file")
    folder.toFile.deleteOnExit()

    val result = recorder().record(maxEvents = 100, folder)

    assert(result.left.exists(_.isInstanceOf[RecordingError.WriteFailed]))
  }
}
