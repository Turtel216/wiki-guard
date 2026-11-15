package dev.wikiguard.stream

import java.time.Clock
import java.util.UUID

import io.prometheus.metrics.exporter.httpserver.HTTPServer
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics
import io.prometheus.metrics.model.registry.PrometheusRegistry
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import dev.wikiguard.common.kafka.RawTopicNames
import dev.wikiguard.stream.lake.LakeSink
import dev.wikiguard.stream.metrics.{MetricsListener, StreamMetrics}

/**
 * Entry point of the streaming application.
 *
 * All streaming queries run in this one application and share its driver and executors.
 */
object StreamApp {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit =
    StreamConfig.fromEnv(sys.env) match {
      case Right(config) => run(config)
      case Left(error) => throw new IllegalStateException(s"Cannot start: ${error.message}")
    }

  private def run(config: StreamConfig): Unit = {
    val spark = SparkSession
      .builder()
      .appName("wikiguard-stream")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    val registry = new PrometheusRegistry
    JvmMetrics.builder().register(registry)
    val metricsServer =
      HTTPServer.builder().port(config.metricsPort).registry(registry).buildAndStart()
    try {
      // Added before any query starts, so that no batch goes unreported.
      spark.streams.addListener(
        new MetricsListener(new StreamMetrics(registry), nameOf(spark), Clock.systemUTC())
      )
      val lakeSink = LakeSink.start(spark, config, RawTopicNames.All)
      logger.info(
        "Started {} with checkpoint {}",
        lakeSink.name,
        config.checkpointOf(lakeSink.name)
      )
      // Returns when a query stops and throws when one fails. Either way the application ends
      // and is started again from its checkpoints.
      spark.streams.awaitAnyTermination()
    } finally metricsServer.close()
  }

  /** Looks up the name of a running query. Only ever called on the driver. */
  private def nameOf(spark: SparkSession)(id: UUID): Option[String] =
    Option(spark.streams.get(id)).flatMap(query => Option(query.name))
}
