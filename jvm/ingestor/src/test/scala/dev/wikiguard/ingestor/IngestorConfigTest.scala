package dev.wikiguard.ingestor

import java.nio.file.Paths

import org.scalatest.funsuite.AnyFunSuite

import dev.wikiguard.common.config.ConfigError
import dev.wikiguard.ingestor.IngestorConfig.Source

class IngestorConfigTest extends AnyFunSuite {

  private val Live = Map("WG_SOURCE" -> "live", "WG_KAFKA_BOOTSTRAP_SERVERS" -> "kafka:9092")

  private val Fixture = Live ++ Map("WG_SOURCE" -> "fixture", "WG_FIXTURE_FILE" -> "events.sse")

  test("the live source needs only a broker address") {
    val expected = IngestorConfig(
      source = Source.Live,
      kafkaBootstrapServers = "kafka:9092",
      wikimediaFile = Paths.get("config/wikimedia.yaml"),
      metricsPort = 9400,
      staysUpAfterReplay = false
    )

    assert(IngestorConfig.fromEnv(Live) == Right(expected))
  }

  test("every setting can be overridden") {
    val env = Fixture ++ Map(
      "WG_REPLAY_SPEED" -> "500",
      "WG_WIKIMEDIA_CONFIG" -> "/etc/wikiguard/wikimedia.yaml",
      "WG_METRICS_PORT" -> "9999",
      "WG_STAY_UP_AFTER_REPLAY" -> "true"
    )

    val expected = IngestorConfig(
      source = Source.Fixture(Paths.get("events.sse"), 500.0),
      kafkaBootstrapServers = "kafka:9092",
      wikimediaFile = Paths.get("/etc/wikiguard/wikimedia.yaml"),
      metricsPort = 9999,
      staysUpAfterReplay = true
    )
    assert(IngestorConfig.fromEnv(env) == Right(expected))
  }

  test("a fixture replays at real speed unless told otherwise") {
    val source = IngestorConfig.fromEnv(Fixture).map(_.source)

    assert(source == Right(Source.Fixture(Paths.get("events.sse"), 1.0)))
  }

  test("there is no default source") {
    assert(IngestorConfig.fromEnv(Live - "WG_SOURCE") == Left(ConfigError.Missing("WG_SOURCE")))
  }

  test("an unknown source is rejected") {
    val config = IngestorConfig.fromEnv(Live + ("WG_SOURCE" -> "wikipedia"))

    assert(config.left.exists(_.isInstanceOf[ConfigError.Invalid]))
  }

  test("a fixture source needs a file") {
    val config = IngestorConfig.fromEnv(Fixture - "WG_FIXTURE_FILE")

    assert(config == Left(ConfigError.Missing("WG_FIXTURE_FILE")))
  }

  test("a blank value counts as not set") {
    val config = IngestorConfig.fromEnv(Live + ("WG_KAFKA_BOOTSTRAP_SERVERS" -> "  "))

    assert(config == Left(ConfigError.Missing("WG_KAFKA_BOOTSTRAP_SERVERS")))
  }

  test("a replay speed must be a number above zero") {
    Seq("0", "-2", "fast", "Infinity").foreach { speed =>
      val config = IngestorConfig.fromEnv(Fixture + ("WG_REPLAY_SPEED" -> speed))
      assert(config.isLeft, s"for speed $speed")
    }
  }

  test("staying up after a replay must be true or false") {
    val config = IngestorConfig.fromEnv(Fixture + ("WG_STAY_UP_AFTER_REPLAY" -> "maybe"))

    assert(config.isLeft)
  }

  test("a metrics port must be a port number") {
    Seq("0", "70000", "http").foreach { port =>
      val config = IngestorConfig.fromEnv(Live + ("WG_METRICS_PORT" -> port))
      assert(config.isLeft, s"for port $port")
    }
  }
}
