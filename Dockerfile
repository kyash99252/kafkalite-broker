FROM eclipse-temurin:latest AS build
WORKDIR /workspace

# Pull Maven binaries into the latest Temurin JDK image
COPY --from=maven:latest /usr/share/maven /usr/share/maven
ENV MAVEN_HOME=/usr/share/maven
ENV PATH="${MAVEN_HOME}/bin:${PATH}"

COPY pom.xml ./
COPY broker-protocol/pom.xml broker-protocol/pom.xml
COPY broker-core/pom.xml broker-core/pom.xml
COPY broker-client/pom.xml broker-client/pom.xml
COPY broker-e2e-tests/pom.xml broker-e2e-tests/pom.xml
COPY broker-benchmarks/pom.xml broker-benchmarks/pom.xml

RUN mvn -B -DskipTests dependency:go-offline

COPY . .
RUN mvn -B -pl broker-core -am package -DskipTests

FROM eclipse-temurin:latest AS runtime
WORKDIR /app

RUN groupadd --system kafkalite && useradd --system --gid kafkalite kafkalite \
    && mkdir -p /data /app/config \
    && chown -R kafkalite:kafkalite /data /app

COPY --from=build /workspace/broker-core/target/kafkalite-broker.jar /app/kafkalite-broker.jar
COPY config/broker.properties /app/config/broker.properties

ENV BROKER_CONFIG=/app/config/broker.properties \
    BROKER_HOST=0.0.0.0 \
    BROKER_PORT=9092 \
    DATA_DIR=/data \
    FLUSH_POLICY=EVERY_WRITE

VOLUME ["/data"]
EXPOSE 9092

USER kafkalite

HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=5 \
    CMD ["java", "-cp", "/app/kafkalite-broker.jar", "com.kafkalite.core.lifecycle.HealthCheck"]

ENTRYPOINT ["java", "-jar", "/app/kafkalite-broker.jar"]