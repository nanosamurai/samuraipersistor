(ns samuraipersistor.webhook-outcome-consumer
  (:require [integrant.core :as ig]
            [clojure.string :as string]
            [jsonista.core :as j]
            [org.corfield.logging4j2 :as log]
            [samuraipersistor.kafka.consumer-loop :as loop]
            [samuraipersistor.otel.traceparent :as tp]
            [samuraipersistor.persist :as persist])
  (:import (java.nio.charset StandardCharsets)
           (java.time Instant)
           (java.util UUID)
           (java.util.concurrent LinkedBlockingQueue)
           (org.apache.kafka.clients.consumer ConsumerRecord)))

(def ^:private json-mapper
  (j/object-mapper {:decode-key-fn keyword}))

(defn- consumer-config
  [{:keys [bootstrap-servers client-id webhook-outcome-consumer-group-id
           max-poll-records max-poll-interval-ms session-timeout-ms security-protocol]}]
  {"bootstrap.servers" bootstrap-servers
   "client.id" (or client-id "samuraipersistor")
   "group.id" webhook-outcome-consumer-group-id
   "security.protocol" (or security-protocol "PLAINTEXT")
   "enable.auto.commit" "false"
   "auto.offset.reset" "earliest"
   "max.poll.records" (str (or max-poll-records 200))
   "max.poll.interval.ms" (str (or max-poll-interval-ms 300000))
   "session.timeout.ms" (str (or session-timeout-ms 30000))
   "key.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"})

(defn- bytes->string ^String [^bytes b]
  (String. b StandardCharsets/UTF_8))

(defn- parse-uuid [s]
  (when (and s (not (string/blank? (str s))))
    (UUID/fromString (str s))))

(defn- parse-instant [x]
  (cond
    (nil? x) nil
    (instance? Instant x) x
    :else (Instant/parse (str x))))

(defn- normalize-outcome
  "Normalize a decoded JSON map into the shape expected by persistence.

  Does basic coercions (UUIDs, created_at Instant) and attaches Kafka provenance." 
  [^ConsumerRecord rec m]
  (let [status (:status m)
        status (cond
                 (nil? status) nil
                 (keyword? status) (name status)
                 (string? status) status
                 :else (str status))]
    {:dispatch-id (parse-uuid (:dispatch_id m))
   :event-id (:event_id m)
   :event-type (:event_type m)
   :tenant-id (parse-uuid (:tenant_id m))
   :session-id (parse-uuid (:session_id m))
   :webhook-id (:webhook_id m)
   :attempt-no (int (or (:attempt_no m) 0))
   :status status
   :http-status (when-let [hs (:http_status m)] (int hs))
   :error-code (:error_code m)
   :error-detail (:error_detail m)
   :latency-ms (when-let [l (:latency_ms m)] (long l))
   :created-at (parse-instant (:created_at m))
   :kafka {:topic (.topic rec)
           :partition (.partition rec)
           :offset (.offset rec)}}))

(defn- valid-outcome?
  "Return true if the outcome contains the minimum required fields.

  If this returns false, the record should be treated as a poison pill: log + commit." 
  [{:keys [dispatch-id tenant-id webhook-id event-type status attempt-no]}]
  (and dispatch-id
       tenant-id
       (seq webhook-id)
       (seq event-type)
       (seq status)
       (number? attempt-no)))

(defn- start-worker!
  [{:keys [queue commit! db]}]
  (let [^LinkedBlockingQueue q queue
        stop? (atom false)
        thread (Thread.
                 (fn []
                   (log/info "Webhook outcome worker started")
                   (try
                     (while (not @stop?)
                       (let [batch-size 200
                             first-rec (loop/take! q)
                             recs (transient [first-rec])]
                         (loop [i 1]
                           (when (< i batch-size)
                             (when-let [r (loop/poll! q 5)]
                               (conj! recs r)
                               (recur (inc i)))))
                         (let [records (persistent! recs)
                               processed (transient [])]
                           (doseq [^ConsumerRecord rec records]
                             (try
                               (tp/with-record-trace rec
                                 (let [raw (bytes->string (.value rec))
                                       m (j/read-value raw json-mapper)
                                       out (normalize-outcome rec m)]
                                   ;; Very important: do not log full error_detail by default.
                                   (log/info "Persisting webhook delivery outcome" {:dispatch-id (:dispatch-id out)
                                                                                     :webhook-id (:webhook-id out)
                                                                                     :tenant-id (:tenant-id out)
                                                                                     :session-id (:session-id out)
                                                                                     :attempt-no (:attempt-no out)
                                                                                     :status (:status out)})
                                   (if (valid-outcome? out)
                                     (persist/insert-webhook-delivery-outcome! (:ds db) out)
                                     (log/warn "Skipping invalid webhook delivery outcome" {:dispatch-id (:dispatch-id out)
                                                                                            :tenant-id (:tenant-id out)
                                                                                            :webhook-id (:webhook-id out)
                                                                                            :event-type (:event-type out)
                                                                                            :status (:status out)
                                                                                            :attempt-no (:attempt-no out)}))
                                   (conj! processed rec)))
                               (catch Throwable t
                                 ;; Poison-pill policy: log + commit, so the group cannot get stuck.
                                 (log/error t "Failed to persist webhook delivery outcome" {:topic (.topic rec)
                                                                                             :partition (.partition rec)
                                                                                             :offset (.offset rec)})
                                 (conj! processed rec))))
                           (commit! (persistent! processed)))))
                     (catch InterruptedException _
                       (log/info "Webhook outcome worker interrupted"))
                     (catch Throwable t
                       (log/error t "Webhook outcome worker crashed"))
                     (finally
                       (log/info "Webhook outcome worker stopped")))))]
    (.setName thread "webhook-outcome-worker")
    (.setDaemon thread true)
    (.start thread)
    {:thread thread
     :stop! (fn []
              (reset! stop? true)
              (.interrupt thread))}))

(defmethod ig/init-key :samuraipersistor/webhook-outcome-consumer
  [_ {:keys [config db]}]
  (let [kcfg (get config :kafka)
        enabled? (not= false (:webhook-outcome-enabled? kcfg))
        topic (get-in kcfg [:topics :webhook-delivery-outcome])]
    (if-not enabled?
      (do
        (log/warn "Webhook outcome consumer disabled" {:reason :config})
        {:enabled? false})
      (do
        (when-not (seq topic)
          (throw (ex-info "Missing Kafka topic for webhook delivery outcomes" {:config-path [:kafka :topics :webhook-delivery-outcome]})))
        (log/info "Starting webhook outcome consumer" {:topic topic})
        (let [consumer (loop/start-consumer-loop!
                         {:consumer-config (consumer-config kcfg)
                          :topic topic
                          :buffer-size (or (:webhook-outcome-buffer-size kcfg) 2000)
                          :name "webhook-outcome"})
              worker (start-worker! {:queue (:queue consumer)
                                     :commit! (:commit! consumer)
                                     :db db})]
          {:enabled? true
           :consumer consumer
           :worker worker})))))

(defmethod ig/halt-key! :samuraipersistor/webhook-outcome-consumer
  [_ {:keys [consumer worker]}]
  (when-let [stop! (get-in worker [:stop!])] (stop!))
  (when-let [stop! (get-in consumer [:stop!])] (stop!)))
