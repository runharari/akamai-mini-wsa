# Build stage: compile and package the jar (tests run separately with ./mvnw verify)
FROM eclipse-temurin:25-jdk AS build
WORKDIR /build
COPY mvnw pom.xml ./
COPY .mvn .mvn
RUN ./mvnw -q -B dependency:go-offline
COPY src src
RUN ./mvnw -q -B package -DskipTests

# Run stage: one image for every role; SPRING_PROFILES_ACTIVE picks ingest, processor, sink or query
FROM eclipse-temurin:25-jre
WORKDIR /app
# curl is only for the compose health check
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 1001 wsa
COPY --from=build /build/target/mini-wsa-*.jar app.jar
USER wsa
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
