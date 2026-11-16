package dev.wikiguard.integration

import java.nio.file.Paths
import java.time.{Clock, Instant}
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.util.Using

import io.prometheus.metrics.model.registry.PrometheusRegistry
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.spark.sql.{functions => F, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.time.{Millis, Seconds, Span}
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.postgresql.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName

import dev.wikiguard.batch.migrate.MigrateLake
import dev.wikiguard.common.lake.LakeTables
import dev.wikiguard.ingestor.{KafkaRawEventSink, RawRecord, SampleEvents}
import dev.wikiguard.stream.StreamConfig
import dev.wikiguard.stream.lake.{BronzeCols, BronzeRow, LakeSink}
import dev.wikiguard.stream.metrics.{MetricsListener, MetricValues, StreamMetrics}

/**
 * Runs the lake-sink query against a real broker, object store and catalog. The events are written
 * the way the ingestor writes them, and the table is created from the DDL files.
 */
class LakeSinkTest extends AnyFunSuite with BeforeAndAfterAll with Eventually {

  import LakeSinkTest.Topics
  import LakeSpark.requiredEnv

  private val LakeBucket = "wg-lake-sink"
  private val CheckpointBucket = "wg-checkpoints-sink"
  private val Table = s"${LakeSpark.Catalog}.${LakeTables.BronzeEventsRaw}"

  private val kafka = new KafkaContainer(DockerImageName.parse(requiredEnv("WG_KAFKA_IMAGE")))

  private val seaweedFs = SeaweedFsContainer(requiredEnv("WG_SEAWEEDFS_IMAGE"))

  private val postgres =
    new PostgreSQLContainer(DockerImageName.parse(requiredEnv("WG_POSTGRES_IMAGE")))
      .withDatabaseName("iceberg")

  private lazy val spark: SparkSession =
    LakeSpark.session("lake-sink-test", postgres, seaweedFs, LakeBucket)

  private val testCounter = new AtomicInteger(0)

  override def beforeAll(): Unit = {
    super.beforeAll()
    kafka.start()
    seaweedFs.start()
    postgres.start()
    seaweedFs.createBucket(LakeBucket)
    seaweedFs.createBucket(CheckpointBucket)
    val migrated =
      MigrateLake.run(spark, LakeSpark.Catalog, Paths.get(requiredEnv("WG_DDL_DIR")))
    require(migrated.isRight, s"could not create the lake tables: $migrated")
  }

  override def afterAll(): Unit =
    try spark.stop()
    finally {
      postgres.stop()
      seaweedFs.stop()
      kafka.stop()
      super.afterAll()
    }

  /** Topics and a checkpoint that no other test uses. All tests share the one table. */
  private def newTopics(): Topics = {
    val suffix = testCounter.incrementAndGet()
    val topics = Topics(s"wg.raw.revision-create.$suffix", s"wg.raw.revision-tags-change.$suffix")
    val settings = new Properties()
    settings.put("bootstrap.servers", kafka.getBootstrapServers)
    Using.resource(Admin.create(settings)) { admin =>
      val requests = topics.all.map(name => new NewTopic(name, 3, 1.toShort))
      admin.createTopics(requests.asJava).all().get()
    }
    topics
  }

  private def config(topics: Topics): StreamConfig =
    StreamConfig(
      kafkaBootstrapServers = kafka.getBootstrapServers,
      checkpointRoot = s"s3a://$CheckpointBucket/${topics.revisions}",
      catalog = LakeSpark.Catalog,
      lakeSinkTrigger = 1.second,
      maxOffsetsPerTrigger = 25L,
      metricsPort = 9400
    )

  private def event(number: Int, topics: Topics, day: String = "2026-11-01"): RawRecord = {
    val isTagsChange = number % 4 == 0
    val time = Instant.parse(s"${day}T10:00:00Z").plusSeconds(number.toLong)
    val body = SampleEvents.body(
      stream = if (isTagsChange) SampleEvents.TagsChange else SampleEvents.RevisionCreate,
      pageId = number.toLong,
      time = time.toString
    )
    val topic = if (isTagsChange) topics.tags else topics.revisions
    RawRecord(topic, Some(number.toLong), Some(s"[$number]"), Some(time), body)
  }

  /** Stores the events the way the ingestor does. */
  private def ingest(topics: Topics, records: Seq[RawRecord]): Unit =
    Using.resource(KafkaRawEventSink(kafka.getBootstrapServers, topics.all)) { sink =>
      records.foreach(record => assert(sink.write(record) == Right(())))
    }

  /**
   * Starts the query, lets it copy everything that is in the topics, and stops it again. Returns
   * how many records it read from Kafka on the way.
   */
  private def copyToLake(topics: Topics): Long = {
    val query = LakeSink.start(spark, config(topics), topics.all)
    try {
      query.processAllAvailable()
      query.recentProgress.map(_.numInputRows).sum
    } finally query.stop()
  }

  private def rows(topics: Topics): Vector[BronzeRow] = {
    import spark.implicits._
    spark
      .table(Table)
      .filter(F.col(BronzeCols.KafkaTopic).isin(topics.all.toSeq: _*))
      .as[BronzeRow]
      .collect()
      .toVector
  }

  test("every event in the raw topics becomes one row with its id, time and body") {
    val topics = newTopics()
    val events = (1 to 60).map(number => event(number, topics))
    ingest(topics, events)

    val read = copyToLake(topics)

    assert(read == 60)
    val stored = rows(topics)
    assert(stored.size == 60)
    assert(stored.map(_.event_id).toSet == events.map(_.eventId).toSet)
    assert(stored.map(row => row.kafka_topic -> row.json).toSet ==
      events.map(e => e.topic -> e.json).toSet)
    assert(stored.map(row => row.event_id -> row.event_ts).toSet ==
      events.map(e => e.eventId -> e.eventTime.getOrElse(Instant.EPOCH)).toSet)
    assert(stored.forall(_.wiki.contains("enwiki")))
    assert(stored.flatMap(_.stream).toSet ==
      Set(SampleEvents.RevisionCreate, SampleEvents.TagsChange))
    assert(stored.forall(row => !row.ingested_at.isBefore(Instant.parse("2026-11-16T00:00:00Z"))))
  }

  test("a restart copies only what is new") {
    val topics = newTopics()
    val events = (1 to 40).map(number => event(number, topics))
    ingest(topics, events.take(15))
    copyToLake(topics)

    ingest(topics, events.drop(15))
    copyToLake(topics)

    assert(rows(topics).map(_.event_id).sorted == events.map(_.eventId).sorted)
  }

  test("a batch that is run again after a crash is not stored twice") {
    val topics = newTopics()
    ingest(topics, (1 to 10).map(number => event(number, topics)))
    copyToLake(topics)
    assert(rows(topics).size == 10)

    // Spark writes this file last. Without it the batch looks unfinished and is run again.
    val commits = new Path(config(topics).checkpointOf(LakeSink.QueryName), "commits")
    val files = commits.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val finished = files.listStatus(commits).toVector.map(_.getPath)
    assert(finished.nonEmpty)
    finished.foreach(file => assert(files.delete(file, false)))
    val readAgain = copyToLake(topics)

    assert(readAgain == 10, "the batch should have been run a second time")
    assert(rows(topics).size == 10)
  }

  test("the query reports what it read and how far behind each topic it is") {
    val topics = newTopics()
    // Three batches of at most 25 records, so the first ones end with records still waiting.
    ingest(topics, (1 to 60).map(number => event(number, topics)))
    val registry = new PrometheusRegistry
    val listener = new MetricsListener(new StreamMetrics(registry), _ => None, Clock.systemUTC())
    val query = "query" -> LakeSink.QueryName
    def lag(topic: String): Option[Double] =
      MetricValues.value(registry, "wg_stream_source_lag_records", query, "topic" -> topic)

    spark.streams.addListener(listener)
    try {
      copyToLake(topics)
      // Spark hands events to listeners on a thread of its own, a moment after a batch ends.
      eventually(timeout(Span(20, Seconds)), interval(Span(100, Millis))) {
        assert(MetricValues.value(registry, "wg_stream_input_rows", query).contains(60.0))
      }
    } finally spark.streams.removeListener(listener)

    assert(MetricValues.value(registry, "wg_stream_batches", query).exists(_ >= 3.0))
    assert(lag(topics.revisions).contains(0.0))
    assert(lag(topics.tags).contains(0.0))
  }

  test("rows are stored as Parquet files, one partition per day of the event") {
    val topics = newTopics()
    import spark.implicits._
    val events = (1 to 5).map(event(_, topics, "2026-10-05")) ++ (6 to 9).map(event(
      _,
      topics,
      "2026-10-06"
    ))
    ingest(topics, events)

    copyToLake(topics)

    val files = spark
      .sql(
        s"SELECT file_format, CAST(partition.event_ts_day AS STRING), record_count FROM $Table.files"
      )
      .as[(String, String, Long)]
      .collect()
      .toVector
      .filter { case (_, day, _) => day.startsWith("2026-10") }
    assert(files.forall { case (format, _, _) => format == "PARQUET" })
    val rowsPerDay = files.groupMapReduce { case (_, day, _) => day } { case (_, _, n) => n }(_ + _)
    assert(rowsPerDay == Map("2026-10-05" -> 5L, "2026-10-06" -> 4L))
  }
}

object LakeSinkTest {

  /** The raw topics of one test, so that tests cannot see each other's records. */
  final case class Topics(revisions: String, tags: String) {
    val all: Set[String] = Set(revisions, tags)
  }
}
