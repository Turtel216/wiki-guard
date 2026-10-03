addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.2")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.10")

// Lets the build read versions.toml from the repository root.
libraryDependencies += "org.tomlj" % "tomlj" % "2.2.0"
