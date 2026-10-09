# syntax=docker/dockerfile:1
# Multi-stage Dockerfile for StreamForge Alerts Java Service

FROM eclipse-temurin:17-jdk-alpine AS builder

WORKDIR /app

COPY pom.xml ./
COPY .mvn/ .mvn/
COPY mvnw ./
RUN chmod +x ./mvnw

COPY src/ src/
RUN ./mvnw clean package -DskipTests -B

FROM eclipse-temurin:17-jre-alpine

RUN addgroup -g 10001 -S streamforge && \
    adduser -u 10001 -S streamforge -G streamforge

WORKDIR /app

COPY --from=builder /app/target/alerts-java-*.jar app.jar
RUN chown -R 10001:10001 /app

USER 10001:10001

EXPOSE 8081

ENTRYPOINT ["java", "-jar", "app.jar"]
