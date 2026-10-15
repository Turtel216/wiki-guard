package dev.wikiguard.ingestor

import org.scalatest.funsuite.AnyFunSuite

class RawTopicsTest extends AnyFunSuite {

  test("only the streams that are asked for get a topic") {
    val topics = RawTopics.forStreams(Seq("mediawiki.revision-create"))

    assert(topics == Right(Map("mediawiki.revision-create" -> "wg.raw.revision-create")))
  }

  test("a stream without a raw topic is rejected") {
    val topics = RawTopics.forStreams(Seq("mediawiki.revision-create", "mediawiki.page-move"))

    assert(topics.left.exists(_.message.contains("mediawiki.page-move")))
  }
}
