package dev.wikiguard.integration

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.{Clock, Duration, Instant}
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.util.Using

import io.prometheus.metrics.model.registry.PrometheusRegistry
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName

import dev.wikiguard.ingestor.{
  Backoff,
  EventRouter,
  FixtureSource,
  IngestError,
  IngestMetrics,
  Ingestor,
  KafkaRawEventSink,
  RawRecord,
  ResumePoint,
  SampleEvents,
  StreamEvent
}

/**
 * Runs the ingestor's Kafka side against a real broker: writing, finding the place to resume from,
 * and a whole replay that is stopped and started again.
 */
class KafkaIngestTest extends AnyFunSuite with BeforeAndAfterAll {

  private val Partitions = 3

  private val kafka = new KafkaContainer(DockerImageName.parse(requiredEnv("WG_KAFKA_IMAGE")))

  private val topicCounter = new AtomicInteger(0)

  override def beforeAll(): Unit = {
    super.beforeAll()
    kafka.start()
  }

  override def afterAll(): Unit = {
    kafka.stop()
    super.afterAll()
  }

  /** The topics of one test, so that tests cannot see each other's records. */
  final private case class Topics(revisions: String, tags: String) {
    val all: Set[String] = Set(revisions, tags)

    val byStream: Map[String, String] =
      Map(SampleEvents.RevisionCreate -> revisions, SampleEvents.TagsChange -> tags)
  }

  private def newTopics(): Topics = {
    val suffix = topicCounter.incrementAndGet()
    val topics = Topics(s"wg.raw.revision-create.$suffix", s"wg.raw.revision-tags-change.$suffix")
    Using.resource(Admin.create(connection())) { admin =>
      val requests = topics.all.map(name => new NewTopic(name, Partitions, 1.toShort))
      admin.createTopics(requests.asJava).all().get()
    }
    topics
  }

  private def connection(): Properties = {
    val settings = new Properties()
    settings.put("bootstrap.servers", kafka.getBootstrapServers)
    settings
  }

  private def withSink[A](topics: Topics)(use: KafkaRawEventSink => A): A =
    Using.resource(KafkaRawEventSink(kafka.getBootstrapServers, topics.all))(use)

  /** Everything that is in the given topics, in the order it was written. */
  private def stored(topics: Topics): Vector[ConsumerRecord[String, String]] =
    Using.resource(
      new KafkaConsumer(connection(), new StringDeserializer, new StringDeserializer)
    ) { consumer =>
      val partitions = for {
        topic <- topics.all.toVector
        partition <- 0 until Partitions
      } yield new TopicPartition(topic, partition)
      consumer.assign(partitions.asJava)
      consumer.seekToBeginning(partitions.asJava)
      val total = consumer.endOffsets(partitions.asJava).asScala.values.map(_.toLong).sum
      drain(consumer, total, Vector.empty, System.nanoTime() + 30.seconds.toNanos)
        .sortBy(record => (record.timestamp, record.offset))
    }

  @tailrec
  private def drain(
      consumer: KafkaConsumer[String, String],
      total: Long,
      found: Vector[ConsumerRecord[String, String]],
      deadline: Long
  ): Vector[ConsumerRecord[String, String]] =
    if (found.size >= total || System.nanoTime() > deadline) {
      found
    } else {
      val batch = consumer.poll(Duration.ofMillis(500)).asScala.toVector
      drain(consumer, total, found ++ batch, deadline)
    }

  private def eventId(record: ConsumerRecord[String, String]): Option[String] =
    Option(record.headers.lastHeader(KafkaRawEventSink.EventIdHeader))
      .map(header => new String(header.value, StandardCharsets.UTF_8))

  private def event(number: Int, stream: String = SampleEvents.RevisionCreate): StreamEvent = {
    val time = Instant.parse("2026-11-01T10:00:00Z").plusSeconds(number.toLong)
    val body = SampleEvents.body(stream = stream, pageId = number.toLong, time = time.toString)
    SampleEvents.event(s"[$number]", body)
  }

  private def recording(events: Seq[StreamEvent]): Path = {
    val file = Files.createTempFile("recording", ".sse")
    file.toFile.deleteOnExit()
    Files.writeString(file, SampleEvents.wire(events))
  }

  /** One whole run of the ingestor over a recording, as fast as it will go. */
  private def replay(file: Path, topics: Topics): Either[IngestError, Unit] =
    withSink(topics) { sink =>
      val timing = Ingestor.Timing(
        backoff = Backoff(1.second, 60.seconds),
        upstreamRetention = 3650.days,
        clock = Clock.systemUTC(),
        sleep = _ => (),
        jitter = () => 1.0
      )
      new Ingestor(
        new FixtureSource(file, speed = 1.0, sleep = _ => ()),
        sink,
        new EventRouter(Set("enwiki"), topics.byStream),
        _ => (),
        new IngestMetrics(new PrometheusRegistry),
        timing
      ).run()
    }

  test("empty topics have nothing to resume from") {
    val topics = newTopics()

    assert(withSink(topics)(_.lastEvent()) == Right(None))
  }

  test("a written record can be read back with its key, body and stream position") {
    val topics = newTopics()
    val record = RawRecord(topics.revisions, Some(7L), Some("[1]"), None, SampleEvents.body())

    assert(withSink(topics)(_.write(record)) == Right(()))

    val records = stored(topics)
    assert(records.map(_.key) == Vector("7"))
    assert(records.map(_.value) == Vector(SampleEvents.body()))
    assert(records.map(eventId) == Vector(Some("[1]")))
  }

  test("the resume point is the last record written, whichever partition it went to") {
    val topics = newTopics()
    val events = (1 to 40).map { number =>
      event(number, if (number % 4 == 0) SampleEvents.TagsChange else SampleEvents.RevisionCreate)
    }

    val resumePoints = withSink(topics) { sink =>
      events.map { e =>
        val topic = if (e.data.contains(SampleEvents.TagsChange)) topics.tags else topics.revisions
        val pageId = e.id.map(_.filter(_.isDigit).toLong)
        assert(sink.write(RawRecord(topic, pageId, e.id, None, e.data)) == Right(()))
        sink.lastEvent().map(_.map(_.eventId))
      }
    }

    assert(resumePoints == events.map(e => Right(e.id)))
    assert(stored(topics).map(_.partition).toSet.size > 1, "the records should spread out")
  }

  test("the resume point carries the time of the event") {
    val topics = newTopics()
    val last = event(5)
    val record = RawRecord(topics.revisions, Some(5L), last.id, None, last.data)

    val resume = withSink(topics) { sink =>
      sink.write(record).flatMap(_ => sink.lastEvent())
    }

    assert(resume == Right(Some(ResumePoint("[5]", Instant.parse("2026-11-01T10:00:05Z")))))
  }

  test("a replay stores every kept event once and in order") {
    val topics = newTopics()
    val kept = (1 to 50).map(number => event(number))
    val canary = SampleEvents.event("[c]", SampleEvents.body(domain = "canary"))
    val german = SampleEvents.event("[d]", SampleEvents.body(wiki = "dewiki"))

    val result = replay(recording(kept.take(25) ++ Seq(canary, german) ++ kept.drop(25)), topics)

    assert(result == Right(()))
    val records = stored(topics)
    assert(records.map(eventId) == kept.map(_.id))
    assert(records.map(_.value) == kept.map(_.data))
    assert(records.map(_.key) == (1 to 50).map(_.toString))
  }

  test("records with the same page id land in the same partition") {
    val topics = newTopics()
    val events = (1 to 30).map { number =>
      val body = SampleEvents.body(pageId = (number % 5).toLong)
      SampleEvents.event(s"[$number]", body)
    }

    assert(replay(recording(events), topics) == Right(()))

    val partitionsByPage = stored(topics).groupMap(_.key)(_.partition).view.mapValues(_.toSet)
    assert(partitionsByPage.values.forall(_.size == 1))
  }

  test("a second run over the same recording writes nothing new") {
    val topics = newTopics()
    val file = recording((1 to 20).map(number => event(number)))

    assert(replay(file, topics) == Right(()))
    assert(replay(file, topics) == Right(()))

    assert(stored(topics).size == 20)
  }

  test("a run that starts where an earlier one stopped writes only what is new") {
    val topics = newTopics()
    val events = (1 to 30).map(number => event(number))

    assert(replay(recording(events.take(12)), topics) == Right(()))
    assert(replay(recording(events), topics) == Right(()))

    assert(stored(topics).map(eventId) == events.map(_.id))
  }

  test("a topic that does not exist is an error, not a silent start from nothing") {
    val topics = newTopics().copy(tags = "wg.raw.never-created")

    val resume = withSink(topics)(_.lastEvent())

    assert(resume.left.exists(_.message.contains("wg.raw.never-created")))
    assert(replay(recording(Seq(event(1))), topics).isLeft)
  }

  private def requiredEnv(name: String): String =
    sys.env.getOrElse(name, sys.error(s"$name is not set; run this test through sbt"))
}
