package dev.wikiguard.common.config

import java.nio.file.{Files, Path}

import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.util.Try

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

/**
 * The parts of config/wikimedia.yaml that the ingestor needs.
 *
 * The API budget section of the same file belongs to the enricher and is not read here.
 */
final case class WikimediaConfig(wikis: Set[String], userAgent: UserAgent, stream: StreamSettings)

object WikimediaConfig {

  def load(file: Path): Either[ConfigError, WikimediaConfig] =
    Try(Files.readString(file)).toEither.left
      .map(error => ConfigError.Unreadable(file.toString, error.toString))
      .flatMap(parse)

  def parse(yaml: String): Either[ConfigError, WikimediaConfig] =
    for {
      root <- readTree(yaml)
      wikis <- strings(root, "wikis")
      repoUrl <- string(root, "user_agent.repo_url")
      stream <- streamSettings(root)
    } yield WikimediaConfig(
      wikis = wikis.toSet,
      userAgent = UserAgent(repoUrl, text(root, "user_agent.contact")),
      stream = stream
    )

  private def streamSettings(root: JsonNode): Either[ConfigError, StreamSettings] =
    for {
      url <- string(root, "stream.url")
      names <- strings(root, "stream.names")
      reconnectMin <- seconds(root, "stream.reconnect_min_seconds")
      reconnectMax <- seconds(root, "stream.reconnect_max_seconds")
      idleTimeout <- seconds(root, "stream.idle_timeout_seconds")
      retentionDays <- positive(root, "stream.upstream_retention_days")
      _ <- Either.cond(
        reconnectMin <= reconnectMax,
        (),
        ConfigError.Invalid("stream.reconnect_max_seconds", "is below the minimum")
      )
    } yield StreamSettings(
      url = url.stripSuffix("/"),
      names = names,
      reconnectMin = reconnectMin,
      reconnectMax = reconnectMax,
      idleTimeout = idleTimeout,
      upstreamRetention = retentionDays.days
    )

  private def readTree(yaml: String): Either[ConfigError, JsonNode] =
    Try(new ObjectMapper(new YAMLFactory()).readTree(yaml)).toEither.left
      .map(error => ConfigError.Unreadable("wikimedia config", error.getMessage))

  /** Follows a dotted key. Jackson returns a "missing" node rather than null on the way. */
  private def node(root: JsonNode, key: String): JsonNode =
    key.split('.').foldLeft(root)((current, part) => current.path(part))

  private def text(root: JsonNode, key: String): Option[String] =
    Option(node(root, key)).filter(_.isTextual).map(_.asText.trim).filter(_.nonEmpty)

  private def string(root: JsonNode, key: String): Either[ConfigError, String] =
    text(root, key).toRight(ConfigError.Missing(key))

  private def strings(root: JsonNode, key: String): Either[ConfigError, Vector[String]] = {
    val values = node(root, key).elements.asScala.toVector
    if (values.isEmpty) {
      Left(ConfigError.Missing(key))
    } else if (values.forall(value => value.isTextual && value.asText.trim.nonEmpty)) {
      Right(values.map(_.asText.trim))
    } else {
      Left(ConfigError.Invalid(key, "every entry must be a non-empty string"))
    }
  }

  private def positive(root: JsonNode, key: String): Either[ConfigError, Long] = {
    val value = node(root, key)
    if (value.isMissingNode) Left(ConfigError.Missing(key))
    else if (value.isIntegralNumber && value.asLong > 0) Right(value.asLong)
    else Left(ConfigError.Invalid(key, "must be a whole number above zero"))
  }

  private def seconds(root: JsonNode, key: String): Either[ConfigError, FiniteDuration] =
    positive(root, key).map(_.seconds)
}
