# The ingestor service. Build it from the repository root after
# "sbt ingestor/stage" has collected the jars; "make image-ingestor" does both.

# Keep in step with images.jre in versions.toml.
FROM eclipse-temurin:21.0.13_9-jre-noble

COPY jvm/ingestor/target/universal/stage/lib /opt/wikiguard/lib
COPY config/wikimedia.yaml /etc/wikiguard/wikimedia.yaml

ENV WG_WIKIMEDIA_CONFIG=/etc/wikiguard/wikimedia.yaml

# Any user that is not root will do. Nothing is written inside the image.
USER 1000:1000

EXPOSE 9400

# The heap gets 70% of the container's memory limit.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=70", "-cp", "/opt/wikiguard/lib/*", "dev.wikiguard.ingestor.IngestorApp"]
