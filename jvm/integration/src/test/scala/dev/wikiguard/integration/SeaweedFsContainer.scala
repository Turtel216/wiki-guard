package dev.wikiguard.integration

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

/** A single-node SeaweedFS with its S3 API switched on. */
final class SeaweedFsContainer private (image: DockerImageName)
    extends GenericContainer[SeaweedFsContainer](image) {

  /** S3 endpoint as seen from the test JVM. Only valid once the container has started. */
  def endpoint: String = s"http://$getHost:${getMappedPort(SeaweedFsContainer.S3Port)}"

  def createBucket(name: String): Unit = {
    val command = s"echo 's3.bucket.create -name $name' | weed shell"
    val result = execInContainer("/bin/sh", "-c", command)
    require(result.getExitCode == 0, s"could not create bucket $name: ${result.getStderr}")
  }
}

object SeaweedFsContainer {

  val AccessKey: String = "wikiguard"
  val SecretKey: String = "wikiguard-secret"

  private val S3Port = 8333

  def apply(image: String): SeaweedFsContainer =
    new SeaweedFsContainer(DockerImageName.parse(image))
      .withCommand("server", "-s3", "-dir=/data")
      .withEnv("AWS_ACCESS_KEY_ID", AccessKey)
      .withEnv("AWS_SECRET_ACCESS_KEY", SecretKey)
      .withExposedPorts(S3Port)
      .waitingFor(Wait.forHttp("/status").forPort(S3Port))
}
