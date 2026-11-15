package dev.wikiguard.stream.metrics

import org.scalatest.funsuite.AnyFunSuite

class KafkaLagTest extends AnyFunSuite {

  test("the lag of a topic is what all its partitions have left to read") {
    val end = """{"wg.raw.revision-create":{"0":100,"1":40,"2":7}}"""
    val latest = """{"wg.raw.revision-create":{"0":130,"1":40,"2":12}}"""

    assert(KafkaLag.byTopic(end, latest) == Map("wg.raw.revision-create" -> 35L))
  }

  test("every topic is counted on its own") {
    val end = """{"a":{"0":5},"b":{"0":9}}"""
    val latest = """{"a":{"0":6},"b":{"0":9}}"""

    assert(KafkaLag.byTopic(end, latest) == Map("a" -> 1L, "b" -> 0L))
  }

  test("a partition without a position counts as fully read") {
    val end = """{"a":{"0":5}}"""
    val latest = """{"a":{"0":8,"1":50},"b":{"0":3}}"""

    assert(KafkaLag.byTopic(end, latest) == Map("a" -> 3L, "b" -> 0L))
  }

  test("a topic that shrank below the position has no lag") {
    assert(KafkaLag.byTopic("""{"a":{"0":50}}""", """{"a":{"0":20}}""") == Map("a" -> 0L))
  }

  test("offsets of other sources and unreadable text give no lag at all") {
    val notKafka = Seq("""{"logOffset":3}""", "17", "", "not json", "[1,2]")

    notKafka.foreach { text =>
      assert(KafkaLag.byTopic(text, text).isEmpty, text)
    }
  }

  test("offsets that are missing altogether give no lag at all") {
    // Spark hands these over as plain Java strings, which can be absent.
    val missing = Option.empty[String].orNull

    assert(KafkaLag.byTopic(missing, missing).isEmpty)
  }
}
