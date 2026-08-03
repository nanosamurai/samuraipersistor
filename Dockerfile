# syntax=docker/dockerfile:1

FROM clojure:temurin-21-tools-deps AS builder

WORKDIR /app

COPY deps.edn build.clj ./
RUN clojure -P

COPY resources ./resources
COPY src ./src

RUN clojure -T:build uber

FROM eclipse-temurin:21-jre AS runtime

WORKDIR /app

RUN useradd -r -u 10001 -g root samuraipersistor

COPY --from=builder /app/target/samuraipersistor.jar /app/samuraipersistor.jar

EXPOSE 8010

USER 10001

ENTRYPOINT ["java", "-jar", "/app/samuraipersistor.jar"]
