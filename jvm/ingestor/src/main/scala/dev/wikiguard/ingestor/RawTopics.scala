package dev.wikiguard.ingestor

import dev.wikiguard.common.config.ConfigError
import dev.wikiguard.common.kafka.RawTopicNames

/** The raw Kafka topic that each upstream stream is written to. */
object RawTopics {

  val ByStream: Map[String, String] = Map(
    "mediawiki.revision-create" -> RawTopicNames.RevisionCreate,
    "mediawiki.revision-tags-change" -> RawTopicNames.RevisionTagsChange
  )

  /** The topics for the given streams. Every stream that is asked for must have one. */
  def forStreams(names: Seq[String]): Either[ConfigError, Map[String, String]] =
    names.find(name => !ByStream.contains(name)) match {
      case Some(name) => Left(ConfigError.Invalid("stream.names", s"$name has no raw topic"))
      case None => Right(ByStream.view.filterKeys(names.contains).toMap)
    }
}
