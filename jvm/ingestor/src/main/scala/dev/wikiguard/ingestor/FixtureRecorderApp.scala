package dev.wikiguard.ingestor

import org.slf4j.LoggerFactory

import dev.wikiguard.common.config.WikimediaConfig

/**
 * Entry point of the fixture recorder: one run saves one recording and exits.
 *
 * It only talks to Kafka. It is safe to run next to a live ingestor.
 */
object FixtureRecorderApp {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit =
    record(sys.env) match {
      case Right(()) => ()
      case Left(reason) => throw new IllegalStateException(s"Nothing was recorded: $reason")
    }

  private def record(env: Map[String, String]): Either[String, Unit] =
    for {
      config <- RecorderConfig.fromEnv(env).left.map(_.message)
      wikimedia <- WikimediaConfig.load(config.wikimediaFile).left.map(_.message)
      topics <- RawTopics.forStreams(wikimedia.stream.names).left.map(_.message)
      reader = RawTopicReader(config.kafkaBootstrapServers, topics.values.toSet)
      summary <- new FixtureRecorder(reader)
        .record(config.maxEvents, config.file)
        .left
        .map(_.message)
    } yield logger.info(
      "Recorded {} events from {} to {} into {}",
      summary.events,
      summary.from.fold("an unknown time")(_.toString),
      summary.to.fold("an unknown time")(_.toString),
      config.file
    )
}
