package dev.wikiguard.ingestor

import java.nio.file.{Path, Paths}

import dev.wikiguard.common.config.{ConfigError, EnvVars}

/**
 * Settings of one ingestor process, taken from environment variables.
 *
 * What to ask Wikimedia for lives in the shared Wikimedia config file. This is only about where
 * this process reads from and writes to.
 */
final case class IngestorConfig(
    source: IngestorConfig.Source,
    kafkaBootstrapServers: String,
    wikimediaFile: Path,
    metricsPort: Int,
    staysUpAfterReplay: Boolean
)

object IngestorConfig {

  import EnvVars.{required, value}

  sealed trait Source

  object Source {

    /** The real Wikimedia stream. Production only. */
    case object Live extends Source

    /** A recording replayed from a file, at a multiple of its real speed. */
    final case class Fixture(file: Path, speed: Double) extends Source
  }

  val SourceKey: String = "WG_SOURCE"
  val KafkaKey: String = "WG_KAFKA_BOOTSTRAP_SERVERS"
  val WikimediaFileKey: String = "WG_WIKIMEDIA_CONFIG"
  val MetricsPortKey: String = "WG_METRICS_PORT"
  val FixtureFileKey: String = "WG_FIXTURE_FILE"
  val ReplaySpeedKey: String = "WG_REPLAY_SPEED"
  val StayUpKey: String = "WG_STAY_UP_AFTER_REPLAY"

  private val DefaultWikimediaFile = "config/wikimedia.yaml"
  private val DefaultMetricsPort = 9400
  private val DefaultReplaySpeed = 1.0

  def fromEnv(env: Map[String, String]): Either[ConfigError, IngestorConfig] =
    for {
      source <- source(env)
      kafka <- required(env, KafkaKey)
      port <- metricsPort(env)
      staysUp <- staysUp(env)
    } yield IngestorConfig(
      source = source,
      kafkaBootstrapServers = kafka,
      wikimediaFile = wikimediaFile(env),
      metricsPort = port,
      staysUpAfterReplay = staysUp
    )

  /** Where the shared Wikimedia config file is. */
  def wikimediaFile(env: Map[String, String]): Path =
    Paths.get(value(env, WikimediaFileKey).getOrElse(DefaultWikimediaFile))

  /** There is no default source, so that nothing connects to Wikimedia by accident. */
  private def source(env: Map[String, String]): Either[ConfigError, Source] =
    required(env, SourceKey).flatMap {
      case "live" => Right(Source.Live)
      case "fixture" =>
        for {
          file <- required(env, FixtureFileKey)
          speed <- replaySpeed(env)
        } yield Source.Fixture(Paths.get(file), speed)
      case other => Left(ConfigError.Invalid(SourceKey, s"'$other' is neither live nor fixture"))
    }

  private def replaySpeed(env: Map[String, String]): Either[ConfigError, Double] =
    value(env, ReplaySpeedKey).fold[Either[ConfigError, Double]](Right(DefaultReplaySpeed)) { raw =>
      raw.toDoubleOption
        .filter(speed => speed > 0 && !speed.isInfinity)
        .toRight(ConfigError.Invalid(ReplaySpeedKey, "must be a number above zero"))
    }

  /**
   * A finished replay normally ends the process. Inside a cluster that would only get it restarted,
   * so there it can be told to stay up and do nothing instead.
   */
  private def staysUp(env: Map[String, String]): Either[ConfigError, Boolean] =
    value(env, StayUpKey).fold[Either[ConfigError, Boolean]](Right(false)) { raw =>
      raw.toBooleanOption.toRight(ConfigError.Invalid(StayUpKey, "must be true or false"))
    }

  private def metricsPort(env: Map[String, String]): Either[ConfigError, Int] =
    value(env, MetricsPortKey).fold[Either[ConfigError, Int]](Right(DefaultMetricsPort)) { raw =>
      raw.toIntOption
        .filter(port => port > 0 && port < 65536)
        .toRight(ConfigError.Invalid(MetricsPortKey, "must be a port number"))
    }
}
