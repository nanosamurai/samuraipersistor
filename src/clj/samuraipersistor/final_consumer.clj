(ns samuraipersistor.final-consumer
  (:require [integrant.core :as ig]
            [org.corfield.logging4j2 :as log]
            [samuraipersistor.kafka.consumer-loop :as loop]
            [samuraipersistor.kafka.common :as kcommon]
            [samuraipersistor.otel.traceparent :as tp]
            [samuraipersistor.persist :as persist])
  (:import (java.util.concurrent LinkedBlockingQueue)
           (org.apache.kafka.clients.consumer ConsumerRecord)
           (org.apache.kafka.clients.producer KafkaProducer)
           (samuraibff.proto SessionTranscript)))

(defn- consumer-config
  [{:keys [bootstrap-servers client-id final-consumer-group-id max-poll-records max-poll-interval-ms session-timeout-ms]}]
  {"bootstrap.servers" bootstrap-servers
   "client.id" (or client-id "samuraipersistor")
   "group.id" final-consumer-group-id
   "enable.auto.commit" "false"
   "auto.offset.reset" "earliest"
   "max.poll.records" (str (or max-poll-records 50))
   "max.poll.interval.ms" (str (or max-poll-interval-ms 300000))
   "session.timeout.ms" (str (or session-timeout-ms 30000))
   "key.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"})

(defn- parse-final ^SessionTranscript [^bytes value]
  (SessionTranscript/parseFrom value))

(defn- record->dlq-payload [^ConsumerRecord rec ^SessionTranscript ev reason]
  {:reason reason
   :kafka {:topic (.topic rec)
           :partition (.partition rec)
           :offset (.offset rec)}
   :event {:type "final"
           :session_id (.getSessionId ev)
           :tenant_id (.getTenantId ev)
           :duration_s (.getDurationS ev)
           :lang (.getLang ev)}})

(defn- start-worker!
  [{:keys [queue commit! db dlq-topic kafka-config]}]
  (let [^LinkedBlockingQueue q queue
        ^KafkaProducer dlq-producer (when dlq-topic (kcommon/->producer kafka-config))
        stop? (atom false)
        thread (Thread.
                 (fn []
                   (log/info "Final worker started")
                   (try
                     (while (not @stop?)
                       (let [batch-size 20
                             first-rec (loop/take! q)
                             recs (transient [first-rec])]
                         (loop [i 1]
                           (when (< i batch-size)
                             (when-let [r (loop/poll! q 10)]
                               (conj! recs r)
                               (recur (inc i)))))
                         (let [records (persistent! recs)
                               processed (transient [])]
                           (doseq [^ConsumerRecord rec records]
                             (try
                               (tp/with-record-trace rec
                                 (let [ev (parse-final (.value rec))
                                       res (persist/insert-final!
                                             (:ds db)
                                             ev
                                             {:source "finalizer_worker"
                                              :model "whisperx"
                                              :event-created-at-ns (when (pos? (.getCreatedAtNs ev))
                                                                    (.getCreatedAtNs ev))})]
                                   (when (= res :missing-session)
                                     (when dlq-producer
                                       (kcommon/send-dlq!
                                         dlq-producer
                                         dlq-topic
                                         (.getSessionId ev)
                                         (record->dlq-payload rec ev "missing_session"))))
                                   (conj! processed rec)))
                               (catch Throwable t
                                 (log/error t "Failed to persist final transcript" {:topic (.topic rec)
                                                                                    :partition (.partition rec)
                                                                                    :offset (.offset rec)}))))
                           (let [processed (persistent! processed)]
                             (commit! processed)))))
                     (catch InterruptedException _
                       (log/info "Final worker interrupted"))
                     (catch Throwable t
                       (log/error t "Final worker crashed"))
                     (finally
                       (when dlq-producer
                         (try (.close dlq-producer) (catch Throwable _)))
                       (log/info "Final worker stopped")))))]
    (.setName thread "final-worker")
    (.setDaemon thread true)
    (.start thread)
    {:thread thread
     :stop! (fn []
              (reset! stop? true)
              (.interrupt thread))}))

(defmethod ig/init-key :samuraipersistor/final-consumer
  [_ {:keys [config db]}]
  (let [kcfg (get config :kafka)
        topic (get-in kcfg [:topics :final])
        dlq-topic (get-in kcfg [:topics :dlq])
        consumer (loop/start-consumer-loop!
                   {:consumer-config (consumer-config kcfg)
                    :topic topic
                    :buffer-size (or (:final-buffer-size kcfg) 200)
                    :name "final"})
        worker (start-worker! {:queue (:queue consumer)
                               :commit! (:commit! consumer)
                               :db db
                               :dlq-topic dlq-topic
                               :kafka-config kcfg})]
    {:consumer consumer
     :worker worker}))

(defmethod ig/halt-key! :samuraipersistor/final-consumer
  [_ {:keys [consumer worker]}]
  (when-let [stop! (get-in worker [:stop!])] (stop!))
  (when-let [stop! (get-in consumer [:stop!])] (stop!)))
