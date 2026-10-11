package dev.wikiguard.ingestor

import scala.concurrent.duration.{Duration, DurationInt}

import org.scalatest.funsuite.AnyFunSuite

class BackoffTest extends AnyFunSuite {

  private val backoff = Backoff(min = 1.second, max = 60.seconds)

  test("there is no wait before any failure") {
    assert(backoff.delay(failures = 0, jitter = 1.0) == Duration.Zero)
  }

  test("the wait doubles with each failure") {
    val waits = (1 to 5).map(failures => backoff.delay(failures, jitter = 1.0))

    assert(waits == Seq(1.second, 2.seconds, 4.seconds, 8.seconds, 16.seconds))
  }

  test("the wait stops growing at the maximum") {
    assert(backoff.delay(failures = 7, jitter = 1.0) == 60.seconds)
    assert(backoff.delay(failures = 1000, jitter = 1.0) == 60.seconds)
  }

  test("jitter shortens the wait by up to half") {
    assert(backoff.delay(failures = 4, jitter = 0.0) == 4.seconds)
    assert(backoff.delay(failures = 4, jitter = 0.5) == 6.seconds)
  }

  test("the wait never drops below the minimum") {
    assert(backoff.delay(failures = 1, jitter = 0.0) == 1.second)
  }
}
