package dev.wikiguard.ingestor

import java.time.Instant

import scala.util.Try

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

/**
 * The few fields the ingestor reads from an event body.
 *
 * Everything except the stream name is optional on purpose. The body is stored untouched, so the
 * ingestor should depend on as little of the upstream schema as it can.
 */
final case class EventEnvelope(
    stream: String,
    domain: Option[String],
    wiki: Option[String],
    pageId: Option[Long],
    time: Option[Instant]
)

object EventEnvelope {

  private val mapper = new ObjectMapper()

  /** Returns nothing when the body is not a JSON object that names its stream. */
  def parse(json: String): Option[EventEnvelope] =
    for {
      root <- Try(mapper.readTree(json)).toOption.flatMap(Option(_))
      meta = root.path("meta")
      stream <- text(meta.path("stream"))
    } yield EventEnvelope(
      stream = stream,
      domain = text(meta.path("domain")),
      wiki = text(root.path("database")),
      pageId = Some(root.path("page_id")).filter(_.isIntegralNumber).map(_.asLong),
      time = text(meta.path("dt")).flatMap(value => Try(Instant.parse(value)).toOption)
    )

  private def text(node: JsonNode): Option[String] =
    Some(node).filter(_.isTextual).map(_.asText).filter(_.nonEmpty)
}
