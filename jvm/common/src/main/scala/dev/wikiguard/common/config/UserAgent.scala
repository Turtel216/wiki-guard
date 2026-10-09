package dev.wikiguard.common.config

/**
 * The identity sent to Wikimedia on every request.
 *
 * The contact is optional here so that the config file can be loaded without one. It becomes
 * mandatory at the moment a header is built, which only happens for real connections.
 */
final case class UserAgent(repoUrl: String, contact: Option[String]) {

  /** Builds the header value, or fails when there is no contact to put in it. */
  def header(version: String): Either[ConfigError, String] =
    contact
      .toRight(ConfigError.Missing("user_agent.contact"))
      .map(address => s"WikiGuard/$version ($repoUrl; $address)")
}
