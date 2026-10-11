package dev.wikiguard.ingestor

import java.io.{BufferedReader, InputStream, InputStreamReader, IOException}
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration.{DurationLong, FiniteDuration}
import scala.jdk.OptionConverters._
import scala.util.Try

import dev.wikiguard.common.config.StreamSettings

/**
 * The live Wikimedia event stream, read over one long HTTP connection.
 *
 * @param userAgent
 *   the full header value that identifies this client to Wikimedia
 * @param watchdog
 *   runs the check that closes a connection which has gone silent
 */
final class SseSource(
    settings: StreamSettings,
    userAgent: String,
    client: HttpClient,
    watchdog: ScheduledExecutorService
) extends EventSource {

  val isEndless: Boolean = true

  def connect[A](resumeFrom: Option[String])(
      consume: Iterator[StreamEvent] => A
  ): Either[SourceError, A] =
    Try(client.send(request(resumeFrom), HttpResponse.BodyHandlers.ofInputStream())).toEither.left
      .map(error => SourceError.Unreachable(error.toString))
      .flatMap { response =>
        if (response.statusCode == 200) {
          read(response.body)(consume)
        } else {
          response.body.close()
          Left(SourceError.Rejected(response.statusCode, retryAfter(response)))
        }
      }

  private def request(resumeFrom: Option[String]): HttpRequest = {
    val base = HttpRequest
      .newBuilder(URI.create(settings.connectUrl))
      .timeout(SseSource.ResponseTimeout)
      .header("Accept", "text/event-stream")
      .header("User-Agent", userAgent)
    resumeFrom.fold(base)(id => base.header("Last-Event-ID", id)).GET().build()
  }

  private def read[A](body: InputStream)(consume: Iterator[StreamEvent] => A) = {
    val lastLineAt = new AtomicLong(System.nanoTime())
    val idleCheck = watchdog.scheduleAtFixedRate(
      () => if (idleFor(lastLineAt) > settings.idleTimeout) body.close(),
      SseSource.CheckEvery.toMillis,
      SseSource.CheckEvery.toMillis,
      TimeUnit.MILLISECONDS
    )
    val reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))
    val lines = Iterator.continually(Option(reader.readLine())).takeWhile(_.isDefined).flatten.map {
      line =>
        lastLineAt.set(System.nanoTime())
        line
    }
    try Right(consume(SseParser.events(lines)))
    catch {
      case error: IOException => Left(SourceError.Interrupted(error.toString))
    } finally {
      idleCheck.cancel(false)
      body.close()
    }
  }

  private def idleFor(lastLineAt: AtomicLong): FiniteDuration =
    (System.nanoTime() - lastLineAt.get).nanos

  /** Only the plain number-of-seconds form is understood. */
  private def retryAfter(response: HttpResponse[_]): Option[FiniteDuration] =
    response.headers
      .firstValue("Retry-After")
      .toScala
      .flatMap(_.trim.toLongOption)
      .filter(_ >= 0)
      .map(_.seconds)
}

object SseSource {

  // How long to wait for the server to start answering. It does not limit the stream itself.
  private val ResponseTimeout = Duration.ofSeconds(30)

  private val CheckEvery = 1.second

  def apply(
      settings: StreamSettings,
      userAgent: String,
      watchdog: ScheduledExecutorService
  ): SseSource = {
    val client = HttpClient.newBuilder().connectTimeout(ResponseTimeout).build()
    new SseSource(settings, userAgent, client, watchdog)
  }
}
