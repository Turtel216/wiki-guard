package dev.wikiguard.ingestor

import java.net.InetSocketAddress
import java.net.http.HttpClient
import java.nio.charset.StandardCharsets
import java.util.concurrent.{Executors, ScheduledExecutorService}
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration.{DurationInt, DurationLong, FiniteDuration}

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import dev.wikiguard.common.config.StreamSettings

/** Runs the source against a small HTTP server on this machine. */
class SseSourceTest extends AnyFunSuite with BeforeAndAfterAll {

  private val watchdog: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  private val events = Vector(SampleEvents.event("[1]"), SampleEvents.event("[2]"))

  override def afterAll(): Unit = {
    watchdog.shutdownNow()
    super.afterAll()
  }

  /** Starts a server that answers every request with `respond`, and passes its base URL on. */
  private def withServer[A](respond: HttpExchange => Unit)(test: String => A): A = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext("/", exchange => respond(exchange))
    server.setExecutor(Executors.newCachedThreadPool())
    server.start()
    try test(s"http://127.0.0.1:${server.getAddress.getPort}/v2/stream")
    finally server.stop(0)
  }

  private def source(url: String, idleTimeout: FiniteDuration = 30.seconds): SseSource = {
    val settings = StreamSettings(
      url = url,
      names = Vector("mediawiki.revision-create", "mediawiki.revision-tags-change"),
      reconnectMin = 1.second,
      reconnectMax = 60.seconds,
      idleTimeout = idleTimeout,
      upstreamRetention = 7.days
    )
    new SseSource(settings, "WikiGuard/test (unit test)", HttpClient.newHttpClient(), watchdog)
  }

  private def send(exchange: HttpExchange, status: Int, body: String): Unit = {
    exchange.sendResponseHeaders(status, 0)
    exchange.getResponseBody.write(body.getBytes(StandardCharsets.UTF_8))
    exchange.close()
  }

  test("events are read until the server closes the connection") {
    withServer(send(_, 200, SampleEvents.wire(events))) { url =>
      assert(source(url).connect(None)(_.toVector) == Right(events))
    }
  }

  test("the request names both streams, the client and the resume point") {
    val seen = new AtomicReference(Map.empty[String, Option[String]])
    def respond(exchange: HttpExchange): Unit = {
      val headers = exchange.getRequestHeaders
      seen.set(
        Map(
          "path" -> Some(exchange.getRequestURI.getPath),
          "agent" -> Option(headers.getFirst("User-Agent")),
          "accept" -> Option(headers.getFirst("Accept")),
          "resume" -> Option(headers.getFirst("Last-Event-ID"))
        )
      )
      send(exchange, 200, "")
    }

    withServer(respond) { url =>
      source(url).connect(Some("[1]"))(_.size)
    }

    val expected = Map(
      "path" -> Some("/v2/stream/mediawiki.revision-create,mediawiki.revision-tags-change"),
      "agent" -> Some("WikiGuard/test (unit test)"),
      "accept" -> Some("text/event-stream"),
      "resume" -> Some("[1]")
    )
    assert(seen.get == expected)
  }

  test("no resume header is sent on a first connection") {
    val resume = new AtomicReference(Option("unset"))
    def respond(exchange: HttpExchange): Unit = {
      resume.set(Option(exchange.getRequestHeaders.getFirst("Last-Event-ID")))
      send(exchange, 200, "")
    }

    withServer(respond) { url =>
      source(url).connect(None)(_.size)
    }

    assert(resume.get.isEmpty)
  }

  test("a throttled connection reports how long to stay away") {
    def respond(exchange: HttpExchange): Unit = {
      exchange.getResponseHeaders.add("Retry-After", "120")
      send(exchange, 429, "slow down")
    }

    withServer(respond) { url =>
      val result = source(url).connect(None)(_.size)

      assert(result == Left(SourceError.Rejected(429, Some(120.seconds))))
      assert(SourceError.Rejected(429, None).isThrottled)
    }
  }

  test("any other status is a rejection without a wait") {
    withServer(send(_, 404, "")) { url =>
      assert(source(url).connect(None)(_.size) == Left(SourceError.Rejected(404, None)))
    }
  }

  test("a server that is not there is unreachable") {
    val url = withServer(send(_, 200, ""))(identity)

    val result = source(url).connect(None)(_.size)

    assert(result.left.exists(_.isInstanceOf[SourceError.Unreachable]))
  }

  test("a connection that goes silent is given up") {
    // Sends one event and then holds the connection open without sending anything more.
    def respond(exchange: HttpExchange): Unit = {
      exchange.sendResponseHeaders(200, 0)
      exchange.getResponseBody.write(SampleEvents.wire(events.take(1)).getBytes("UTF-8"))
      exchange.getResponseBody.flush()
      Thread.sleep(5000)
    }

    withServer(respond) { url =>
      val started = System.nanoTime()
      val result = source(url, idleTimeout = 1.second).connect(None)(_.toVector)
      val took = (System.nanoTime() - started).nanos

      // Depending on timing the read ends quietly or with an error. Both are fine.
      assert(result.forall(_ == events.take(1)))
      assert(took < 15.seconds)
    }
  }
}
