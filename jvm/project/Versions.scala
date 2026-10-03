import java.nio.file.Paths

import org.tomlj.Toml

/**
 * Reads pinned versions from versions.toml in the repository root.
 *
 * sbt is always started from the jvm directory, so the file sits one level up.
 */
object Versions {

  private val toml = Toml.parse(Paths.get("..", "versions.toml"))

  /** Looks up a dotted key such as "jvm.spark" and fails the build if it is missing. */
  def apply(key: String): String =
    Option(toml.getString(key)).getOrElse(sys.error(s"versions.toml has no entry for '$key'"))
}
