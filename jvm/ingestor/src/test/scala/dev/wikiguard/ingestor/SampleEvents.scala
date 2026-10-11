package dev.wikiguard.ingestor

/**
 * Hand-written events shaped like the upstream ones. They are not recordings, and they carry only
 * the fields the ingestor looks at plus a few for realism.
 */
object SampleEvents {

  val RevisionCreate: String = "mediawiki.revision-create"
  val TagsChange: String = "mediawiki.revision-tags-change"

  def body(
      stream: String = RevisionCreate,
      wiki: String = "enwiki",
      domain: String = "en.wikipedia.org",
      pageId: Long = 42L,
      time: String = "2026-11-01T10:00:00Z"
  ): String =
    s"""{"meta":{"stream":"$stream","domain":"$domain","dt":"$time"},""" +
      s""""database":"$wiki","page_id":$pageId,"rev_id":1001}"""

  def event(id: String, data: String = body()): StreamEvent = StreamEvent(Some(id), data)

  /** The same events as text on the wire. */
  def wire(events: Seq[StreamEvent]): String =
    events.map { event =>
      val id = event.id.fold("")(value => s"id: $value\n")
      s"event: message\n${id}data: ${event.data}\n\n"
    }.mkString
}
