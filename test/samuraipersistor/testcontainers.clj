(ns samuraipersistor.testcontainers
  "Small helpers for running Postgres+Kafka with Testcontainers.

  We deliberately use fixed host ports so tests can use simple JDBC/Kafka URIs.
  This avoids interacting with any real services you might have on 5432/9092." 
  (:import (org.testcontainers.containers PostgreSQLContainer KafkaContainer)
           (org.testcontainers.utility DockerImageName)
           (org.testcontainers.containers.wait.strategy Wait)
           (java.time Duration)))

(def postgres-image "postgres:16")
(def kafka-image "confluentinc/cp-kafka:7.6.1")

(defn start-postgres! []
  (doto (PostgreSQLContainer. (DockerImageName/parse postgres-image))
    (.withDatabaseName "drsynth")
    (.withUsername "drsynth")
    (.withPassword "drsynth")
    ;; fixed host port mapping
    (.setPortBindings ["127.0.0.1:15432:5432"])
    (.waitingFor (-> (Wait/forListeningPort)
                     (.withStartupTimeout (Duration/ofSeconds 60))))
    (.start)))

(defn start-kafka! []
  ;; KafkaContainer picks a random host port by default. We pin it for deterministic tests.
  ;; Note: Kafka container requires access to Docker; on Windows this needs Docker Desktop.
  (doto (KafkaContainer. (DockerImageName/parse kafka-image))
    (.setPortBindings ["127.0.0.1:19092:9093"]) ; KafkaContainer exposes 9093 internally
    (.waitingFor (-> (Wait/forListeningPort)
                     (.withStartupTimeout (Duration/ofSeconds 90))))
    (.start)))

(defn stop! [c]
  (when c
    (.stop c)))
