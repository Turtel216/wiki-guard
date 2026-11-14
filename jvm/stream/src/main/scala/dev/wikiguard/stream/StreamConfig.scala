package dev.wikiguard.stream

import scala.concurrent.duration.{DurationLong, FiniteDuration}

import dev.wikiguard.common.config.{ConfigError, EnvVars}
import dev.wikiguard.common.lake.LakeCatalog

/**
 * Settings of the streaming application, taken from environment variables.
 *
 * How Spark reaches the catalog and the object store is Spark configuration and is not repeated
 * here.
 *
 * @param checkpointRoot
 *   every query keeps its checkpoint in a directory of its own below this one
 * @param catalog
 *   the name under which Spark knows the Iceberg catalog of the lake
 * @param lakeSinkTrigger
 *   how often new records are appended to the lake
 * @param maxOffsetsPerTrigger
 *   the most Kafka records one batch may take, so that catching up stays bounded
 */
final case class StreamConfig(
    kafkaBootstrapServers: String,
    checkpointRoot: String,
    catalog: String,
    lakeSinkTrigger: FiniteDuration,
    maxOffsetsPerTrigger: Long
) {

  def checkpointOf(query: String): String = s"$checkpointRoot/$query"
}

object StreamConfig {

  import EnvVars.{required, value}

  val KafkaKey: String = "WG_KAFKA_BOOTSTRAP_SERVERS"
  val CheckpointRootKey: String = "WG_CHECKPOINT_ROOT"
  val LakeSinkTriggerKey: String = "WG_LAKE_SINK_TRIGGER_SECONDS"
  val MaxOffsetsKey: String = "WG_MAX_OFFSETS_PER_TRIGGER"

  private val DefaultLakeSinkTriggerSeconds = 60L
  private val DefaultMaxOffsets = 20000L

  def fromEnv(env: Map[String, String]): Either[ConfigError, StreamConfig] =
    for {
      kafka <- required(env, KafkaKey)
      checkpointRoot <- required(env, CheckpointRootKey)
      catalog <- LakeCatalog.fromEnv(env)
      triggerSeconds <- positive(env, LakeSinkTriggerKey, DefaultLakeSinkTriggerSeconds)
      maxOffsets <- positive(env, MaxOffsetsKey, DefaultMaxOffsets)
    } yield StreamConfig(
      kafkaBootstrapServers = kafka,
      checkpointRoot = checkpointRoot.stripSuffix("/"),
      catalog = catalog,
      lakeSinkTrigger = triggerSeconds.seconds,
      maxOffsetsPerTrigger = maxOffsets
    )

  private def positive(
      env: Map[String, String],
      key: String,
      default: Long
  ): Either[ConfigError, Long] =
    value(env, key).fold[Either[ConfigError, Long]](Right(default)) { raw =>
      raw.toLongOption
        .filter(_ > 0)
        .toRight(ConfigError.Invalid(key, "must be a whole number above zero"))
    }
}
