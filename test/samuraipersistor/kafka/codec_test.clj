(ns samuraipersistor.kafka.codec-test
  "Exercise the runtime Kafka compression and serialization dependencies."
  (:require [clojure.test :refer [deftest is]]
            [jsonista.core :as json]
            [samuraipersistor.testcontainers :as tc])
  (:import (java.time Duration)
           (java.util UUID)
           (java.util.concurrent TimeUnit)
           (org.apache.kafka.clients.consumer KafkaConsumer)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (samuraibff.proto RefinedEvent)))

(deftest lz4-records-preserve-protobuf-and-json
  (let [broker (tc/start-kafka!)
        topic (str "codec-" (UUID/randomUUID))
        event (-> (RefinedEvent/newBuilder)
                  (.setSessionId "synthetic-session")
                  (.setTenantId "synthetic-tenant")
                  (.setTrackId "synthetic-track")
                  (.setText "synthetic transcript")
                  (.build))
        audit {"tenant_id" "synthetic-tenant" "attempt_no" 2}]
    (try
      (with-open [producer (KafkaProducer.
                            {"bootstrap.servers" (.getBootstrapServers broker)
                             "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                             "value.serializer" "org.apache.kafka.common.serialization.ByteArraySerializer"
                             "compression.type" "lz4"
                             "acks" "all"})
                  consumer (KafkaConsumer.
                            {"bootstrap.servers" (.getBootstrapServers broker)
                             "group.id" (str "codec-" (UUID/randomUUID))
                             "auto.offset.reset" "earliest"
                             "enable.auto.commit" "false"
                             "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                             "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"})]
        (doseq [[key value] [["protobuf" (.toByteArray event)]
                             ["json" (json/write-value-as-bytes audit)]]]
          (.get (.send producer (ProducerRecord. topic key value)) 30 TimeUnit/SECONDS))
        (.subscribe consumer [topic])
        (let [deadline (+ (System/nanoTime) (.toNanos (Duration/ofSeconds 30)))
              records (loop [received {}]
                        (if (or (= 2 (count received)) (> (System/nanoTime) deadline))
                          received
                          (recur (reduce (fn [acc record]
                                           (assoc acc (.key record) (.value record)))
                                         received (.poll consumer (Duration/ofMillis 250))))))]
          (is (= #{"protobuf" "json"} (set (keys records))))
          (when (= 2 (count records))
            (is (= event (RefinedEvent/parseFrom ^bytes (get records "protobuf"))))
            (is (= audit (json/read-value (get records "json")))))))
      (finally
        (tc/stop! broker)))))
