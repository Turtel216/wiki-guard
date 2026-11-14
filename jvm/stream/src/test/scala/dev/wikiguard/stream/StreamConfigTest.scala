package dev.wikiguard.stream

import scala.concurrent.duration.DurationInt

import org.scalatest.funsuite.AnyFunSuite

class StreamConfigTest extends AnyFunSuite {

  private val minimal = Map(
    StreamConfig.KafkaKey -> "kafka:9092",
    StreamConfig.CheckpointRootKey -> "s3a://wg-checkpoints/"
  )

  test("only Kafka and the checkpoint root have to be set") {
    val config = StreamConfig.fromEnv(minimal)

    assert(config ==
      Right(StreamConfig("kafka:9092", "s3a://wg-checkpoints", "lake", 60.seconds, 20000L)))
  }

  test("every query gets a checkpoint directory of its own") {
    val checkpoint = StreamConfig.fromEnv(minimal).map(_.checkpointOf("lake-sink"))

    assert(checkpoint == Right("s3a://wg-checkpoints/lake-sink"))
  }

  test("the trigger and the batch size can be changed") {
    val env = minimal ++ Map(
      StreamConfig.LakeSinkTriggerKey -> "5",
      StreamConfig.MaxOffsetsKey -> "500"
    )

    val config = StreamConfig.fromEnv(env)

    assert(config.map(_.lakeSinkTrigger) == Right(5.seconds))
    assert(config.map(_.maxOffsetsPerTrigger) == Right(500L))
  }

  test("a missing setting is reported by its key") {
    val config = StreamConfig.fromEnv(minimal - StreamConfig.CheckpointRootKey)

    assert(config.left.exists(_.message.contains(StreamConfig.CheckpointRootKey)))
  }

  test("a trigger that is not a number above zero is rejected") {
    Seq("0", "-1", "soon").foreach { raw =>
      val config = StreamConfig.fromEnv(minimal + (StreamConfig.LakeSinkTriggerKey -> raw))

      assert(config.left.exists(_.message.contains(StreamConfig.LakeSinkTriggerKey)), raw)
    }
  }
}
