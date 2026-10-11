package dev.wikiguard.ingestor

/** The raw Kafka topic that each upstream stream is written to. */
object RawTopics {

  val ByStream: Map[String, String] = Map(
    "mediawiki.revision-create" -> "wg.raw.revision-create",
    "mediawiki.revision-tags-change" -> "wg.raw.revision-tags-change"
  )
}
