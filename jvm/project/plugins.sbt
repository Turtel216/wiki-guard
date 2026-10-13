addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.2")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.10")
// Collects a service and its libraries in one directory for the container image.
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.7")

// Lets the build read versions.toml from the repository root.
libraryDependencies += "org.tomlj" % "tomlj" % "2.2.0"
