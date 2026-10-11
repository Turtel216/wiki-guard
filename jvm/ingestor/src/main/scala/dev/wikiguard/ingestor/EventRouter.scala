package dev.wikiguard.ingestor

/**
 * Decides whether an event is kept and which raw topic it goes to.
 *
 * @param wikis
 *   the wikis to keep, by database name such as "enwiki"
 * @param topics
 *   the raw topic for each upstream stream name
 */
final class EventRouter(wikis: Set[String], topics: Map[String, String]) {

  def route(event: StreamEvent): Either[DropReason, RawRecord] =
    for {
      envelope <- EventEnvelope.parse(event.data).toRight(DropReason.Malformed)
      _ <- Either.cond(!envelope.domain.contains(EventRouter.CanaryDomain), (), DropReason.Canary)
      wiki <- envelope.wiki.toRight(DropReason.Malformed)
      _ <- Either.cond(wikis.contains(wiki), (), DropReason.OtherWiki)
      topic <- topics.get(envelope.stream).toRight(DropReason.UnknownStream)
    } yield RawRecord(topic, envelope.pageId, event.id, envelope.time, event.data)
}

object EventRouter {

  private val CanaryDomain = "canary"
}
