package dev.wikiguard.ingestor

import java.nio.file.{Path, Paths}

import dev.wikiguard.common.config.{ConfigError, EnvVars}

/**
 * Settings of one run of the fixture recorder, taken from environment variables.
 *
 * @param file
 *   where the recording is written; an existing file is replaced
 * @param maxEvents
 *   how many of the newest events to record
 */
final case class RecorderConfig(
    kafkaBootstrapServers: String,
    wikimediaFile: Path,
    file: Path,
    maxEvents: Int
)

object RecorderConfig {

  import EnvVars.{required, value}

  val FileKey: String = "WG_RECORD_FILE"
  val MaxEventsKey: String = "WG_RECORD_MAX_EVENTS"

  private val DefaultMaxEvents = 1000

  // The whole recording is held in memory before it is written, so it cannot be endless.
  private val MostEvents = 50000

  def fromEnv(env: Map[String, String]): Either[ConfigError, RecorderConfig] =
    for {
      kafka <- required(env, IngestorConfig.KafkaKey)
      file <- required(env, FileKey)
      maxEvents <- maxEvents(env)
    } yield RecorderConfig(
      kafkaBootstrapServers = kafka,
      wikimediaFile = IngestorConfig.wikimediaFile(env),
      file = Paths.get(file),
      maxEvents = maxEvents
    )

  private def maxEvents(env: Map[String, String]): Either[ConfigError, Int] =
    value(env, MaxEventsKey).fold[Either[ConfigError, Int]](Right(DefaultMaxEvents)) { raw =>
      raw.toIntOption
        .filter(count => count > 0 && count <= MostEvents)
        .toRight(ConfigError.Invalid(MaxEventsKey, s"must be a number from 1 to $MostEvents"))
    }
}
