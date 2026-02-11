FROM clojure:temurin-21-tools-deps AS builder

WORKDIR /app

COPY deps.edn build.clj ./
COPY src ./src

RUN clojure -T:build compile-java

FROM clojure:temurin-21-tools-deps

WORKDIR /app

COPY deps.edn ./
COPY resources ./resources
COPY src ./src
COPY --from=builder /app/target/classes ./target/classes

EXPOSE 8010

CMD ["clojure", "-M:run"]