package dev.wikiguard.stream

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import dev.wikiguard.common.kafka.RawTopicNames
import dev.wikiguard.stream.lake.LakeSink

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
    val lakeSink = LakeSink.start(spark, config, RawTopicNames.All)
    logger.info("Started {} with checkpoint {}", lakeSink.name, config.checkpointOf(lakeSink.name))
    // Returns when a query stops and throws when one fails. Either way the application ends
    // and is started again from its checkpoints.
    spark.streams.awaitAnyTermination()
  }
}
