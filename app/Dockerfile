FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -q -B dependency:go-offline
COPY src src
RUN --mount=type=cache,target=/root/.m2 mvn -q -B -DskipTests package

FROM eclipse-temurin:21.0.5_11-jre
RUN useradd --system --uid 10001 --no-create-home app
WORKDIR /app
COPY --from=build /src/target/paycore.jar app.jar
USER app
EXPOSE 8080 8081
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=70", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/app.jar"]
