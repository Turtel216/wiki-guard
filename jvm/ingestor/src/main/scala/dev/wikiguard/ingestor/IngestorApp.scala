package dev.wikiguard.ingestor

import java.time.Clock
import java.util.concurrent.{CountDownLatch, Executors, ScheduledExecutorService, ThreadLocalRandom}

import scala.concurrent.duration.FiniteDuration

import io.prometheus.metrics.exporter.httpserver.HTTPServer
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics
import io.prometheus.metrics.model.registry.PrometheusRegistry
import org.slf4j.LoggerFactory

import dev.wikiguard.common.config.{ConfigError, WikimediaConfig}

/** Entry point of the ingestor service. */
object IngestorApp {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit =
    start(sys.env) match {
      case Right(()) => logger.info("The recording has been replayed to its end")
      case Left(reason) => throw new IllegalStateException(s"Ingestor stopped: $reason")
    }

  private def start(env: Map[String, String]): Either[String, Unit] =
    for {
      config <- IngestorConfig.fromEnv(env).left.map(_.message)
      wikimedia <- WikimediaConfig.load(config.wikimediaFile).left.map(_.message)
      topics <- topicsFor(wikimedia).left.map(_.message)
      source <- source(config.source, wikimedia).left.map(_.message)
      result <- run(config, wikimedia, topics, source).left.map(_.message)
    } yield result

  private def run(
      config: IngestorConfig,
      wikimedia: WikimediaConfig,
      topics: Map[String, String],
      source: EventSource
  ): Either[IngestError, Unit] = {
    val registry = new PrometheusRegistry
    JvmMetrics.builder().register(registry)
    val metricsServer =
      HTTPServer.builder().port(config.metricsPort).registry(registry).buildAndStart()
    val timing = Ingestor.Timing(
      backoff = Backoff(wikimedia.stream.reconnectMin, wikimedia.stream.reconnectMax),
      upstreamRetention = wikimedia.stream.upstreamRetention,
      clock = Clock.systemUTC(),
      sleep = sleep,
      jitter = () => ThreadLocalRandom.current().nextDouble()
    )
    val sink = KafkaRawEventSink(config.kafkaBootstrapServers, topics.values.toSet)
    val ingestor = new Ingestor(
      source,
      sink,
      new EventRouter(wikimedia.wikis, topics),
      new LoggingGapRecorder,
      new IngestMetrics(registry),
      timing
    )
    logger.info("Ingesting {} for {}", wikimedia.stream.names, wikimedia.wikis)
    try ingestor.run().map(_ => if (config.staysUpAfterReplay) idle())
    finally {
      sink.close()
      metricsServer.close()
    }
  }

  /** Every stream that is asked for must have a raw topic to go to. */
  private def topicsFor(wikimedia: WikimediaConfig): Either[ConfigError, Map[String, String]] =
    wikimedia.stream.names.find(name => !RawTopics.ByStream.contains(name)) match {
      case Some(name) => Left(ConfigError.Invalid("stream.names", s"$name has no raw topic"))
      case None => Right(RawTopics.ByStream.view.filterKeys(wikimedia.stream.names.contains).toMap)
    }

  private def source(
      choice: IngestorConfig.Source,
      wikimedia: WikimediaConfig
  ): Either[ConfigError, EventSource] =
    choice match {
      case IngestorConfig.Source.Fixture(file, speed) =>
        Right(new FixtureSource(file, speed, sleep))
      case IngestorConfig.Source.Live =>
        wikimedia.userAgent.header(version).map { userAgent =>
          SseSource(wikimedia.stream, userAgent, watchdog())
        }
    }

  /** A background thread that must not keep the process alive on its own. */
  private def watchdog(): ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor { task =>
      val thread = new Thread(task, "stream-watchdog")
      thread.setDaemon(true)
      thread
    }

  /** Keeps the process and its metrics endpoint alive with nothing left to do. */
  private def idle(): Unit = {
    logger.info("The recording has been replayed to its end; staying up")
    new CountDownLatch(1).await()
  }

  private def sleep(time: FiniteDuration): Unit = Thread.sleep(time.toMillis)

  /** The version stamped into the jar when it was built, or "dev" when run from classes. */
  private def version: String =
    Option(
      getClass.getPackage
    ).flatMap(pkg => Option(pkg.getImplementationVersion)).getOrElse("dev")
}
