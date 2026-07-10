(ns samuraipersistor.workflow-outcome-consumer
  (:require [integrant.core :as ig]
            [clojure.string :as string]
            [jsonista.core :as j]
            [org.corfield.logging4j2 :as log]
            [samuraipersistor.kafka.consumer-loop :as loop]
            [samuraipersistor.otel.traceparent :as tp]
            [samuraipersistor.persist :as persist])
  (:import (java.nio.charset StandardCharsets)
           (java.util UUID)
           (java.util.concurrent LinkedBlockingQueue)
           (org.apache.kafka.clients.consumer ConsumerRecord)))

(def ^:private json-mapper
  (j/object-mapper {:decode-key-fn keyword}))

(defn- consumer-config
  [{:keys [bootstrap-servers client-id workflow-outcome-consumer-group-id
           max-poll-records max-poll-interval-ms session-timeout-ms security-protocol]}]
  {"bootstrap.servers" bootstrap-servers
   "client.id" (or client-id "samuraipersistor")
   "group.id" workflow-outcome-consumer-group-id
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
       (not= false (:workflow-outcome-enabled? kcfg))))

(defn- bytes->string ^String [^bytes b]
  (String. b StandardCharsets/UTF_8))

(defn- parse-uuid* [s]
  (when (and s (not (string/blank? (str s))))
    (UUID/fromString (str s))))

(defn- normalize-outcome
  "Normalize a decoded JSON map into the shape expected by persistence.

  Attaches Kafka provenance."
  [^ConsumerRecord rec m]
  (let [status (:status m)
        status (cond
                 (nil? status) nil
                 (keyword? status) (name status)
                 (string? status) status
                 :else (str status))]
    {:workflow-run-id (parse-uuid* (:workflow_run_id m))
     :tenant-id (parse-uuid* (:tenant_id m))
     :session-id (parse-uuid* (:session_id m))
     :workflow-id (parse-uuid* (:workflow_id m))
     :attempt-no (int (or (:attempt_no m) 0))
     :status status
     :latency-ms (when-let [l (:latency_ms m)] (long l))
     :retry-to-topic (:retry_to_topic m)
     :error-code (:error_code m)
     :error-detail (:error_detail m)
     :created-at (persist/parse-instant (:created_at m))
     :kafka {:topic (.topic rec)
             :partition (.partition rec)
             :offset (.offset rec)}}))

(defn- valid-outcome?
  "Return true if the outcome contains the minimum required fields.

  If this returns false, the record should be treated as a poison pill: log + commit."
  [{:keys [workflow-run-id tenant-id session-id workflow-id status attempt-no]}]
  (and workflow-run-id tenant-id session-id workflow-id (seq status) (number? attempt-no)))

(defn- start-worker!
  [{:keys [queue commit! db]}]
  (let [^LinkedBlockingQueue q queue
        stop? (atom false)
        thread (Thread.
                (fn []
                  (log/info "Workflow outcome worker started")
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
                                    out (normalize-outcome rec m)]
                                (log/info "Persisting workflow outcome"
                                          {:workflow-run-id (:workflow-run-id out)
                                           :tenant-id (:tenant-id out)
                                           :session-id (:session-id out)
                                           :workflow-id (:workflow-id out)
                                           :attempt-no (:attempt-no out)
                                           :status (:status out)})
                                (if (valid-outcome? out)
                                  (persist/insert-workflow-outcome! (:ds db) out)
                                  (log/warn "Skipping invalid workflow outcome"
                                            {:workflow-run-id (:workflow-run-id out)
                                             :tenant-id (:tenant-id out)
                                             :session-id (:session-id out)
                                             :workflow-id (:workflow-id out)
                                             :attempt-no (:attempt-no out)
                                             :status (:status out)}))
                                (conj! processed rec)))
                            (catch Throwable t
                               ;; Poison-pill policy: log + commit, so the group cannot get stuck.
                              (log/error t "Failed to persist workflow outcome" {:topic (.topic rec)
                                                                                 :partition (.partition rec)
                                                                                 :offset (.offset rec)})
                              (conj! processed rec))))
                        (commit! (persistent! processed))))
                    (catch InterruptedException _
                      (log/info "Workflow outcome worker interrupted"))
                    (catch Throwable t
                      (log/error t "Workflow outcome worker crashed"))
                    (finally
                      (log/info "Workflow outcome worker stopped")))))]
    (.setName thread "workflow-outcome-worker")
    (.setDaemon thread true)
    (.start thread)
    {:thread thread
     :stop! (fn []
              (reset! stop? true)
              (.interrupt thread))}))

(defmethod ig/init-key :samuraipersistor/workflow-outcome-consumer
  [_ {:keys [config db]}]
  (let [kcfg (get config :kafka)
        topic (get-in kcfg [:topics :workflow-outcome])]
    (if-not (enabled? kcfg)
      (do
        (log/info "Workflow outcome consumer disabled" {:ce-mode? (:ce-mode? kcfg)
                                                        :enabled? (:workflow-outcome-enabled? kcfg)})
        {:enabled? false})
      (do
        (when-not (seq topic)
          (throw (ex-info "Missing Kafka topic for workflow.outcome" {:config-path [:kafka :topics :workflow-outcome]})))
        (log/info "Starting workflow outcome consumer" {:topic topic})
        (let [consumer (loop/start-consumer-loop!
                        {:consumer-config (consumer-config kcfg)
                         :topic topic
                         :buffer-size (or (:workflow-outcome-buffer-size kcfg) 2000)
                         :name "workflow-outcome"})
              worker (start-worker! {:queue (:queue consumer)
                                     :commit! (:commit! consumer)
                                     :db db})]
          {:enabled? true
           :consumer consumer
           :worker worker})))))

(defmethod ig/halt-key! :samuraipersistor/workflow-outcome-consumer
  [_ {:keys [consumer worker]}]
  (when-let [stop! (get-in worker [:stop!])] (stop!))
  (when-let [stop! (get-in consumer [:stop!])] (stop!)))
