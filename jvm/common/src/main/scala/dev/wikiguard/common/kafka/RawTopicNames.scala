package dev.wikiguard.common.kafka

/**
 * What the writer and the readers of the raw topics have to agree on.
 *
 * The ingestor writes these topics and Spark reads them, and the two share no other code.
 */
object RawTopicNames {

  val RevisionCreate: String = "wg.raw.revision-create"
  val RevisionTagsChange: String = "wg.raw.revision-tags-change"

  /** Every raw topic that holds upstream events. */
  val All: Set[String] = Set(RevisionCreate, RevisionTagsChange)

  /** Name of the record header that holds the stream position of the event. */
  val EventIdHeader: String = "wg-stream-event-id"
}
