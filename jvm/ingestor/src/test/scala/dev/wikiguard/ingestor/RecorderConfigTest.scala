package dev.wikiguard.ingestor

import java.nio.file.Paths

import org.scalatest.funsuite.AnyFunSuite

import dev.wikiguard.common.config.ConfigError

class RecorderConfigTest extends AnyFunSuite {

  private val Minimal =
    Map("WG_KAFKA_BOOTSTRAP_SERVERS" -> "kafka:9092", "WG_RECORD_FILE" -> "fixtures/hour.sse")

  test("a broker address and a file are enough") {
    val expected = RecorderConfig(
      kafkaBootstrapServers = "kafka:9092",
      wikimediaFile = Paths.get("config/wikimedia.yaml"),
      file = Paths.get("fixtures/hour.sse"),
      maxEvents = 1000
    )

    assert(RecorderConfig.fromEnv(Minimal) == Right(expected))
  }

  test("every setting can be overridden") {
    val env = Minimal ++ Map(
      "WG_WIKIMEDIA_CONFIG" -> "/etc/wikiguard/wikimedia.yaml",
      "WG_RECORD_MAX_EVENTS" -> "250"
    )

    val config = RecorderConfig.fromEnv(env)

    assert(config.map(_.wikimediaFile) == Right(Paths.get("/etc/wikiguard/wikimedia.yaml")))
    assert(config.map(_.maxEvents) == Right(250))
  }

  test("the broker address and the file must be set") {
    Seq("WG_KAFKA_BOOTSTRAP_SERVERS", "WG_RECORD_FILE").foreach { key =>
      assert(RecorderConfig.fromEnv(Minimal - key) == Left(ConfigError.Missing(key)))
    }
  }

  test("the number of events must be a sensible whole number") {
    Seq("0", "-5", "many", "2.5", "50001").foreach { count =>
      val config = RecorderConfig.fromEnv(Minimal + ("WG_RECORD_MAX_EVENTS" -> count))
      assert(config.isLeft, s"for $count events")
    }
  }
}
