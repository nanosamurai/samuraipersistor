(ns samuraipersistor.final-track-consumer
  "Bounded final-result consumers with retry of the same uncommitted record."
  (:require [integrant.core :as ig]
            [jsonista.core :as json]
            [org.corfield.logging4j2 :as log]
            [samuraipersistor.final-track-contract :as contract]
            [samuraipersistor.final-tracks :as tracks]
            [samuraipersistor.kafka.common :as common]
            [samuraipersistor.persist :as persist])
  (:import (java.time Duration)
           (java.util.concurrent TimeUnit)
           (com.google.protobuf InvalidProtocolBufferException)
           (org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord ConsumerRebalanceListener OffsetAndMetadata)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.common.errors WakeupException)
           (samuraibff.proto SessionTranscript)))

(defn- consumer-config
  "Build a one-record poll configuration; JDBC work is bounded by statement timeout."
  [config primary?]
  {"bootstrap.servers" (:bootstrap-servers config)
   "group.id" (if primary? (:final-consumer-group-id config) "samuraipersistor-final-tracks")
   "security.protocol" (or (:security-protocol config) "PLAINTEXT")
   "enable.auto.commit" "false" "auto.offset.reset" "earliest"
   "max.poll.records" "1" "max.poll.interval.ms" "300000"
   "max.partition.fetch.bytes" "1048576" "fetch.max.bytes" "5242880"
   "key.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"})

(defn publish-primary!
  "Acknowledge legacy primary publication from committed Postgres content.
  A crash after delivery can repeat publication; consumers must tolerate duplicates."
  [producer topic accepted ^ConsumerRecord input]
  (when-let [transcript (tracks/primary-projection accepted)]
    (let [record (ProducerRecord. topic (.getBytes (str (:session_id accepted)) "UTF-8")
                                  (.toByteArray transcript))]
      (.add (.headers record) "x-result-id" (.getBytes (str (:result_id accepted)) "UTF-8"))
      (.add (.headers record) "x-final-track-contract" (.getBytes "2" "UTF-8"))
      (doseq [header (.headers input)
              :when (and (contains? #{"traceparent" "tracestate"} (.key header))
                         (.value header) (<= (alength ^bytes (.value header)) 512))]
        (.add (.headers record) header))
      (.get (.send producer record) 30 TimeUnit/SECONDS))))

(defn persist-record!
  "Accept one result, then acknowledge primary publication before the input commit."
  [ds config primary? ^ConsumerRecord record producer]
  (let [storage {:bucket (:final-source-bucket config)
                 :recording-prefix (or (:final-recording-prefix config) "recordings")}
        value (.value record)
        key (when-let [bytes (.key record)] (String. ^bytes bytes "UTF-8"))]
    (when (or (nil? value) (> (alength ^bytes value) (if primary? 1000000 contract/max-result-bytes))
              (> (+ (alength ^bytes value) (if-let [key (.key record)] (alength ^bytes key) 0)
                    (reduce + 128 (map #(+ (count (.key %)) 10 (if-let [v (.value %)] (alength ^bytes v) 0))
                                       (.headers record)))) contract/max-record-bytes))
      (contract/invalid! :event-size))
    (if primary?
      (let [event (SessionTranscript/parseFrom ^bytes value)
            headers (contract/projection-headers record)]
        (when (and headers (not= (.getSessionId event) key))
          (contract/invalid! :record-key))
        (persist/insert-final! ds event (merge {:source "finalizer_worker" :model "whisperx"
                                                :event-created-at-ns (.getCreatedAtNs event)
                                                :storage storage} headers)))
      (let [event (contract/outcome value storage)]
        (when-not (= (:session-id event) key)
          (contract/invalid! :record-key))
        (let [accepted (tracks/insert-outcome! ds event)]
          (when (map? accepted)
            (publish-primary! producer (get-in config [:topics :final]) accepted record))
          accepted)))))

(defn- reject-record!
  "Acknowledge a metadata-only DLQ record before allowing the input commit."
  [^KafkaProducer producer topic ^ConsumerRecord record reason]
  (when producer
    (let [value (json/write-value-as-bytes {:reason reason :kafka {:topic (.topic record)
                                                                   :partition (.partition record)
                                                                   :offset (.offset record)}})
          delivery (.send producer (ProducerRecord. topic (.key record) value))]
      (.get delivery 30 TimeUnit/SECONDS)))
  (log/warn "Rejected final persistence record" {:reason reason :topic (.topic record)
                                                 :partition (.partition record) :offset (.offset record)}))

(defn start!
  "Start a final consumer. Polling, retries and commits all stay on one thread.
  A rebalance discards pending local ownership; SQL identities make replay safe."
  [config db primary?]
  (let [topic (if primary? (get-in config [:topics :final]) "transcripts.final-tracks")
        consumer (KafkaConsumer. (consumer-config config primary?))
        dlq-topic (get-in config [:topics :dlq])
        dlq (when dlq-topic (common/->producer config))
        publisher (when-not primary? (common/->producer config))
        stop? (atom false)
        pending (atom nil)
        thread (Thread.
                (fn []
                  (try
                    (.subscribe consumer [topic]
                                (reify ConsumerRebalanceListener
                                  (onPartitionsRevoked [_ _partitions] (reset! pending nil))
                                  (onPartitionsAssigned [_ _partitions] (reset! pending nil))))
                    (log/info "Final persistence consumer started" {:topic topic})
                    (while (not @stop?)
                      (let [records (.poll consumer (Duration/ofMillis 250))]
                        (when-not @pending
                          (when-let [record (first records)]
                            (reset! pending record)))
                        (when-let [^ConsumerRecord record @pending]
                          (.pause consumer (.assignment consumer))
                          (try
                            (let [result (try
                                           (persist-record! (:ds db) config primary? record publisher)
                                           (catch InvalidProtocolBufferException _ :invalid-contract)
                                           (catch clojure.lang.ExceptionInfo error
                                             (if (= :samuraipersistor.final-track-contract/invalid (:type (ex-data error)))
                                               :invalid-contract
                                               (throw error))))]
                              (when (contains? #{:missing-session :invalid-contract} result)
                                (reject-record! dlq dlq-topic record (name result)))
                              (.commitSync consumer {(TopicPartition. (.topic record) (.partition record))
                                                     (OffsetAndMetadata. (inc (.offset record)))})
                              (reset! pending nil)
                              (.resume consumer (.assignment consumer)))
                            (catch Exception error
                              (log/warn "Retrying uncommitted final persistence record"
                                        {:topic topic :partition (.partition record) :offset (.offset record)
                                         :error-kind (.getSimpleName (class error))})
                              (Thread/sleep 1000))))))
                    (catch WakeupException error (when-not @stop? (throw error)))
                    (finally
                      (.close consumer)
                      (when dlq (.close dlq))
                      (when publisher (.close publisher))
                      (log/info "Final persistence consumer stopped" {:topic topic})))))]
    (.setName thread (str "persist-" topic))
    (.setDaemon thread true)
    (.start thread)
    {:thread thread :stop! #(do (reset! stop? true) (.wakeup consumer))}))

(defmethod ig/init-key :samuraipersistor/final-track-consumer
  [_ {:keys [config db]}]
  (when (true? (get-in config [:kafka :final-tracks-enabled?]))
    (start! (:kafka config) db false)))

(defmethod ig/halt-key! :samuraipersistor/final-track-consumer
  [_ component]
  (when-let [stop! (:stop! component)] (stop!)))
