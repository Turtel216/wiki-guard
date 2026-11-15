package dev.wikiguard.stream.metrics

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

/**
 * Works out how far a query is behind its Kafka topics.
 *
 * Spark reports the offsets of a Kafka source as text such as {"topic":{"0":120,"1":98}}, once for
 * where the batch stopped reading and once for the end of the topics.
 */
object KafkaLag {

  private val mapper = new ObjectMapper()

  /**
   * The number of records per topic that were waiting when the batch stopped reading.
   *
   * Offsets of other kinds of sources, and text that cannot be read at all, give an empty result. A
   * partition the batch has no position for counts as fully read.
   */
  def byTopic(endOffsets: String, latestOffsets: String): Map[String, Long] = {
    val read = offsets(endOffsets)
    offsets(latestOffsets).map { case (topic, latest) =>
      val positions = read.getOrElse(topic, Map.empty[String, Long])
      val waiting = latest.map { case (partition, end) =>
        math.max(0L, end - positions.getOrElse(partition, end))
      }
      topic -> waiting.sum
    }
  }

  /** Topic, then partition, then offset. Anything that does not have this shape is left out. */
  private def offsets(json: String): Map[String, Map[String, Long]] =
    Try(mapper.readTree(json)).toOption
      .flatMap(Option(_))
      .map(entries)
      .getOrElse(Vector.empty)
      .collect {
        case (topic, partitions) if partitions.isObject => topic -> partitionOffsets(partitions)
      }
      .toMap

  private def partitionOffsets(partitions: JsonNode): Map[String, Long] =
    entries(partitions).collect {
      case (partition, offset) if offset.isIntegralNumber => partition -> offset.asLong
    }.toMap

  private def entries(node: JsonNode): Vector[(String, JsonNode)] =
    node.properties().asScala.toVector.map(entry => entry.getKey -> entry.getValue)
}
