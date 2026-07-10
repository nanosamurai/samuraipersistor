(ns samuraipersistor.workflow-result-consumer
  (:require [integrant.core :as ig]
            [clojure.string :as string]
            [jsonista.core :as j]
            [org.corfield.logging4j2 :as log]
            [samuraipersistor.kafka.consumer-loop :as loop]
            [samuraipersistor.kafka.common :as kcommon]
            [samuraipersistor.otel.traceparent :as tp]
            [samuraipersistor.persist :as persist])
  (:import (java.nio.charset StandardCharsets)
           (java.util UUID)
           (java.util.concurrent LinkedBlockingQueue)
           (org.apache.kafka.clients.consumer ConsumerRecord)
           (org.apache.kafka.clients.producer KafkaProducer)))

(def ^:private json-mapper
  (j/object-mapper {:decode-key-fn keyword}))

(defn- consumer-config
  [{:keys [bootstrap-servers client-id workflow-result-consumer-group-id
           max-poll-records max-poll-interval-ms session-timeout-ms security-protocol]}]
  {"bootstrap.servers" bootstrap-servers
   "client.id" (or client-id "samuraipersistor")
   "group.id" workflow-result-consumer-group-id
   "security.protocol" (or security-protocol "PLAINTEXT")
   "enable.auto.commit" "false"
   "auto.offset.reset" "earliest"
   "max.poll.records" (str (or max-poll-records 200))
   "max.poll.interval.ms" (str (or max-poll-interval-ms 300000))
   "session.timeout.ms" (str (or session-timeout-ms 30000))
   "key.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"})

(defn- enabled?
  [kcfg]
  (and (false? (:ce-mode? kcfg))
       (not= false (:workflow-result-enabled? kcfg))))

(defn- bytes->string ^String [^bytes b]
  (String. b StandardCharsets/UTF_8))

(defn- parse-uuid* [s]
  (when (and s (not (string/blank? (str s))))
    (UUID/fromString (str s))))

(defn- normalize-result
  "Normalize a decoded JSON map into the shape expected by persistence.

  Attaches Kafka provenance.

  NOTE: We intentionally do not include markdown/json in logs."
  [^ConsumerRecord rec m]
  (let [trigger (:trigger m)
        provider (:provider m)
        usage (:usage m)
        render (:render m)
        stream (:stream m)
        created-at (persist/parse-instant (:created_at m))]
    {:workflow-run-id (parse-uuid* (:workflow_run_id m))
     :tenant-id (parse-uuid* (:tenant_id m))
     :session-id (parse-uuid* (:session_id m))
     :workflow-id (parse-uuid* (:workflow_id m))

     :trigger-type (:type trigger)
     :trigger-source-event-id (:source_event_id trigger)

     :status (let [s (:status m)]
               (cond
                 (keyword? s) (name s)
                 (string? s) s
                 :else (str s)))

     :render-markdown (get render :markdown)
     :render-json (get render :json)

     :provider-type (:type provider)
     :provider-model-id (:model_id provider)

     :usage-input-tokens (:input_tokens usage)
     :usage-output-tokens (:output_tokens usage)

     :stream-source-uri (:source_uri stream)
     :stream-source-node-id (:source_node_id stream)

     :error-code (:error_code m)
     :error-detail (:error_detail m)

     :created-at created-at
     :kafka {:topic (.topic rec)
             :partition (.partition rec)
             :offset (.offset rec)}}))

(defn- valid-result?
  "Return true if the workflow result contains the minimum required fields.

  If this returns false, the record should be treated as a poison pill: log + commit."
  [{:keys [workflow-run-id tenant-id session-id workflow-id status]}]
  (and workflow-run-id tenant-id session-id workflow-id (seq status)))

(defn- record->dlq-payload
  "Build a DLQ JSON payload for a workflow result.

  Must not include workflow output / markdown due to PII risk."
  [^ConsumerRecord rec {:keys [workflow-run-id tenant-id session-id workflow-id trigger-type status]} reason]
  {:reason reason
   :kafka {:topic (.topic rec)
           :partition (.partition rec)
           :offset (.offset rec)}
   :event {:type "workflow.result"
           :workflow_run_id (when workflow-run-id (str workflow-run-id))
           :tenant_id (when tenant-id (str tenant-id))
           :session_id (when session-id (str session-id))
           :workflow_id (when workflow-id (str workflow-id))
           :trigger_type trigger-type
           :status status}})

(defn- start-worker!
  [{:keys [queue commit! db dlq-topic kafka-config]}]
  (let [^LinkedBlockingQueue q queue
        ^KafkaProducer dlq-producer (when dlq-topic (kcommon/->producer kafka-config))
        stop? (atom false)
        thread (Thread.
                (fn []
                  (log/info "Workflow result worker started")
                  (try
                    (while (not @stop?)
                      (let [batch-size 200
                            first-rec (loop/take! q)
                            recs (loop [i 1
                                        recs (transient [first-rec])]
                                   (if (>= i batch-size)
                                     recs
                                     (if-let [r (loop/poll! q 5)]
                                       (recur (inc i) (conj! recs r))
                                       recs)))
                            records (persistent! recs)
                            processed (transient [])]
                        (doseq [^ConsumerRecord rec records]
                          (try
                            (tp/with-record-trace rec
                              (let [raw (bytes->string (.value rec))
                                    m (j/read-value raw json-mapper)
                                    res (normalize-result rec m)]
                                (log/info "Persisting workflow result"
                                          {:workflow-run-id (:workflow-run-id res)
                                           :tenant-id (:tenant-id res)
                                           :session-id (:session-id res)
                                           :workflow-id (:workflow-id res)
                                           :trigger-type (:trigger-type res)
                                           :status (:status res)})
                                (if-not (valid-result? res)
                                  (log/warn "Skipping invalid workflow result"
                                            {:topic (.topic rec)
                                             :partition (.partition rec)
                                             :offset (.offset rec)})
                                  (let [r (persist/insert-workflow-result! (:ds db) res)]
                                    (when (= r :missing-session)
                                      (when dlq-producer
                                        (kcommon/send-dlq!
                                         dlq-producer
                                         dlq-topic
                                         (str (:session-id res))
                                         (record->dlq-payload rec res "missing_session"))))))
                                (conj! processed rec)))
                            (catch Throwable t
                               ;; Poison-pill policy: log + commit, so the group cannot get stuck.
                              (log/error t "Failed to persist workflow result" {:topic (.topic rec)
                                                                                :partition (.partition rec)
                                                                                :offset (.offset rec)})
                              (conj! processed rec))))
                        (commit! (persistent! processed))))
                    (catch InterruptedException _
                      (log/info "Workflow result worker interrupted"))
                    (catch Throwable t
                      (log/error t "Workflow result worker crashed"))
                    (finally
                      (when dlq-producer
                        (try (.close dlq-producer) (catch Throwable _)))
                      (log/info "Workflow result worker stopped")))))]
    (.setName thread "workflow-result-worker")
    (.setDaemon thread true)
    (.start thread)
    {:thread thread
     :stop! (fn []
              (reset! stop? true)
              (.interrupt thread))}))

(defmethod ig/init-key :samuraipersistor/workflow-result-consumer
  [_ {:keys [config db]}]
  (let [kcfg (get config :kafka)
        topic (get-in kcfg [:topics :workflow-result])
        dlq-topic (get-in kcfg [:topics :dlq])]
    (if-not (enabled? kcfg)
      (do
        (log/info "Workflow result consumer disabled" {:ce-mode? (:ce-mode? kcfg)
                                                       :enabled? (:workflow-result-enabled? kcfg)})
        {:enabled? false})
      (do
        (when-not (seq topic)
          (throw (ex-info "Missing Kafka topic for workflow.result" {:config-path [:kafka :topics :workflow-result]})))
        (log/info "Starting workflow result consumer" {:topic topic})
        (let [consumer (loop/start-consumer-loop!
                        {:consumer-config (consumer-config kcfg)
                         :topic topic
                         :buffer-size (or (:workflow-result-buffer-size kcfg) 2000)
                         :name "workflow-result"})
              worker (start-worker! {:queue (:queue consumer)
                                     :commit! (:commit! consumer)
                                     :db db
                                     :dlq-topic dlq-topic
                                     :kafka-config kcfg})]
          {:enabled? true
           :consumer consumer
           :worker worker})))))

(defmethod ig/halt-key! :samuraipersistor/workflow-result-consumer
  [_ {:keys [consumer worker]}]
  (when-let [stop! (get-in worker [:stop!])] (stop!))
  (when-let [stop! (get-in consumer [:stop!])] (stop!)))
