(ns samuraipersistor.kafka.common
  (:require [org.corfield.logging4j2 :as log]
            [jsonista.core :as j])
  (:import (org.apache.kafka.clients.producer ProducerRecord KafkaProducer)
           (org.apache.kafka.common.header.internals RecordHeader)
           (java.nio.charset StandardCharsets)))

(def ^:private json-mapper (j/object-mapper {:encode-key-fn name}))

(defn ->producer
  [{:keys [bootstrap-servers client-id]}]
  (KafkaProducer.
    {"bootstrap.servers" bootstrap-servers
     "client.id" (or client-id "samuraipersistor")
     "acks" "all"
     "compression.type" "zstd"
     "linger.ms" "5"
     "batch.size" "131072"
     "key.serializer" "org.apache.kafka.common.serialization.ByteArraySerializer"
     "value.serializer" "org.apache.kafka.common.serialization.ByteArraySerializer"}))

(defn send-dlq!
  "Send a DLQ payload as JSON.

  `payload` should be an EDN map; will be JSON-encoded.
  Uses key as UTF-8 bytes." 
  [^KafkaProducer producer dlq-topic key payload]
  (let [value (.getBytes (j/write-value-as-string payload json-mapper) StandardCharsets/UTF_8)
        record (ProducerRecord. dlq-topic (.getBytes (str key) StandardCharsets/UTF_8) value)]
    ;; Ensure downstream can parse, even without schema registry.
    (.headers record)
    (.add (.headers record) (RecordHeader. "content-type" (.getBytes "application/json" StandardCharsets/UTF_8)))
    (.send producer record)
    (log/warn "Sent DLQ message" {:topic dlq-topic :key (str key)})))
