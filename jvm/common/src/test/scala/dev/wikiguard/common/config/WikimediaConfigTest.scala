package dev.wikiguard.common.config

import java.nio.file.Paths

import scala.concurrent.duration.DurationInt

import org.scalatest.funsuite.AnyFunSuite

class WikimediaConfigTest extends AnyFunSuite {

  private val Valid =
    """wikis:
      |  - enwiki
      |user_agent:
      |  repo_url: "https://example.org/repo"
      |  contact: "ops@example.org"
      |stream:
      |  url: https://stream.example.org/v2/stream/
      |  names:
      |    - mediawiki.revision-create
      |    - mediawiki.revision-tags-change
      |  reconnect_min_seconds: 1
      |  reconnect_max_seconds: 60
      |  idle_timeout_seconds: 90
      |  upstream_retention_days: 7
      |""".stripMargin

  test("a complete file is parsed") {
    val expected = WikimediaConfig(
      wikis = Set("enwiki"),
      userAgent = UserAgent("https://example.org/repo", Some("ops@example.org")),
      stream = StreamSettings(
        url = "https://stream.example.org/v2/stream",
        names = Vector("mediawiki.revision-create", "mediawiki.revision-tags-change"),
        reconnectMin = 1.second,
        reconnectMax = 60.seconds,
        idleTimeout = 90.seconds,
        upstreamRetention = 7.days
      )
    )

    assert(WikimediaConfig.parse(Valid) == Right(expected))
  }

  test("all stream names share one connection URL") {
    val url = WikimediaConfig.parse(Valid).map(_.stream.connectUrl)

    assert(
      url == Right(
        "https://stream.example.org/v2/stream/" +
          "mediawiki.revision-create,mediawiki.revision-tags-change"
      )
    )
  }

  test("an empty contact loads but cannot produce a User-Agent header") {
    val config = WikimediaConfig.parse(Valid.replace("\"ops@example.org\"", "\"\""))

    assert(config.map(_.userAgent.contact) == Right(None))
    assert(
      config.flatMap(_.userAgent.header("1.0")) == Left(ConfigError.Missing("user_agent.contact"))
    )
  }

  test("the User-Agent header names the version, the repository and the contact") {
    val header = WikimediaConfig.parse(Valid).flatMap(_.userAgent.header("1.2.3"))

    assert(header == Right("WikiGuard/1.2.3 (https://example.org/repo; ops@example.org)"))
  }

  test("a missing setting is reported by its key") {
    val withoutWikis = Valid.replace("wikis:\n  - enwiki\n", "")

    assert(WikimediaConfig.parse(withoutWikis) == Left(ConfigError.Missing("wikis")))
  }

  test("a number that is not above zero is rejected") {
    val config =
      WikimediaConfig.parse(Valid.replace("idle_timeout_seconds: 90", "idle_timeout_seconds: 0"))

    assert(config.left.map(_.message).swap.exists(_.contains("stream.idle_timeout_seconds")))
  }

  test("a maximum backoff below the minimum is rejected") {
    val config =
      WikimediaConfig.parse(Valid.replace("reconnect_min_seconds: 1", "reconnect_min_seconds: 90"))

    assert(config.isLeft)
  }

  test("text that is not YAML is reported as unreadable") {
    assert(WikimediaConfig.parse("wikis: [").left.exists(_.isInstanceOf[ConfigError.Unreadable]))
  }

  test("the file in the repository loads") {
    // Tests run in the module directory, two levels below the repository root.
    val config = WikimediaConfig.load(Paths.get("..", "..", "config", "wikimedia.yaml"))

    assert(config.map(_.wikis) == Right(Set("enwiki")))
  }
}
