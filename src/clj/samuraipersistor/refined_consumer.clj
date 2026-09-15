(ns samuraipersistor.refined-consumer
  (:require [integrant.core :as ig]
            [org.corfield.logging4j2 :as log]
            [samuraipersistor.kafka.consumer-loop :as loop]
            [samuraipersistor.kafka.common :as kcommon]
            [samuraipersistor.otel.traceparent :as tp]
            [samuraipersistor.persist :as persist])
  (:import (java.util.concurrent LinkedBlockingQueue)
           (org.apache.kafka.clients.consumer ConsumerRecord)
           (org.apache.kafka.clients.producer KafkaProducer)
           (samuraibff.proto RefinedEvent)))

(defn- consumer-config
  [{:keys [bootstrap-servers client-id refined-consumer-group-id max-poll-records max-poll-interval-ms session-timeout-ms security-protocol]}]
  {"bootstrap.servers" bootstrap-servers
   "client.id" (or client-id "samuraipersistor")
   "group.id" refined-consumer-group-id
   ;; TLS / security
   ;; For MSK TLS-only (port 9094), set to "SSL".
   "security.protocol" (or security-protocol "PLAINTEXT")
   "enable.auto.commit" "false"
   "auto.offset.reset" "earliest"
   "max.poll.records" (str (or max-poll-records 200))
   "max.poll.interval.ms" (str (or max-poll-interval-ms 300000))
   "session.timeout.ms" (str (or session-timeout-ms 30000))
   "key.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"})

(defn- parse-refined ^RefinedEvent [^bytes value]
  (RefinedEvent/parseFrom value))

(defn- record->dlq-payload [^ConsumerRecord rec ^RefinedEvent ev reason]
  {:reason reason
   :kafka {:topic (.topic rec)
           :partition (.partition rec)
           :offset (.offset rec)}
   :event {:type "refined"
           :session_id (.getSessionId ev)
           :tenant_id (.getTenantId ev)
           :start_s (.getStartS ev)
           :end_s (.getEndS ev)
           ;; do NOT include transcript text by default
           :lang (.getLang ev)}})

(defn- start-worker!
  "Persist refined records in order, retrying a failed write before taking another.
  Accepts queue/commit callback/DB/Kafka dependencies; returns thread and stop!."
  [{:keys [queue commit! db dlq-topic kafka-config]}]
  (let [^LinkedBlockingQueue q queue
        ^KafkaProducer dlq-producer (when dlq-topic (kcommon/->producer kafka-config))
        stop? (atom false)
        thread (Thread.
                 (fn []
                   (log/info "Refined worker started")
                   (try
                     (while (not @stop?)
                      (let [^ConsumerRecord rec (loop/take! q)]
                        (loop []
                          (when-not @stop?
                            (let [stored? (try
                               (tp/with-record-trace rec
                                 (let [ev (parse-refined (.value rec))
                                       res (persist/insert-refined!
                                                         (:ds db) ev
                                                         {:source "whisperx_worker"})]
                                                (when (and (not= res :ok) dlq-producer)
                                       (kcommon/send-dlq!
                                                   dlq-producer dlq-topic (.getSessionId ev)
                                                   (record->dlq-payload rec ev (name res))))))
                                            true
                                            (catch InterruptedException e (throw e))
                                            (catch Exception e
                                              (log/warn "Refined persistence failed; retrying before advancing"
                                                        {:partition (.partition rec) :offset (.offset rec)
                                                         :error-class (.getName (class e))
                                                         :sql-state (when (instance? java.sql.SQLException e)
                                                                      (.getSQLState ^java.sql.SQLException e))})
                                              false))]
                              (if stored?
                                (commit! [rec])
                                (do (Thread/sleep 1000) (recur))))))))
                     (catch InterruptedException _
                       (log/info "Refined worker interrupted"))
                     (catch Throwable t
                       (log/error t "Refined worker crashed"))
                     (finally
                       (when dlq-producer
                         (try (.close dlq-producer) (catch Throwable _)))
                       (log/info "Refined worker stopped")))))]
    (.setName thread "refined-worker")
    (.setDaemon thread true)
    (.start thread)
    {:thread thread
     :stop! (fn [] (reset! stop? true) (.interrupt thread))}))
(defmethod ig/init-key :samuraipersistor/refined-consumer
  [_ {:keys [config db]}]
  (let [kcfg (get config :kafka)
        topic (get-in kcfg [:topics :refined])
        dlq-topic (get-in kcfg [:topics :dlq])
        consumer (loop/start-consumer-loop!
                   {:consumer-config (consumer-config kcfg)
                    :topic topic
                    :buffer-size (or (:refined-buffer-size kcfg) 2000)
                    :name "refined"})
        worker (start-worker! {:queue (:queue consumer)
                               :commit! (:commit! consumer)
                               :db db
                               :dlq-topic dlq-topic
                               :kafka-config kcfg})]
    {:consumer consumer
     :worker worker}))

(defmethod ig/halt-key! :samuraipersistor/refined-consumer
  [_ {:keys [consumer worker]}]
  (when-let [stop! (get-in worker [:stop!])] (stop!))
  (when-let [stop! (get-in consumer [:stop!])] (stop!)))
