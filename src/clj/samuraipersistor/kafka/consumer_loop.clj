(ns samuraipersistor.kafka.consumer-loop
  (:require [org.corfield.logging4j2 :as log])
  (:import (java.time Duration)
           (java.util.concurrent LinkedBlockingQueue TimeUnit)
           (org.apache.kafka.clients.consumer ConsumerRecord ConsumerRecords KafkaConsumer OffsetAndMetadata)
           (org.apache.kafka.common TopicPartition)))

(defn records->commit-req
  "Convert Kafka records to a thread-safe commit request.

  Returns a map of {[topic partition] next-offset} where next-offset is (offset + 1)." 
  [records]
  (when (seq records)
    (reduce (fn [acc ^ConsumerRecord r]
              (let [k [(.topic r) (.partition r)]
                    next-off (inc (.offset r))]
                (update acc k (fnil max 0) next-off)))
            {}
            records)))

(defn- commit-reqs!
  "Commit a merged commit-req map on the consumer.

  Must be called from the consumer polling thread." 
  [^KafkaConsumer consumer commit-req]
  (when (seq commit-req)
    (let [commit-map (into {}
                           (map (fn [[[topic partition] off]]
                                  [(TopicPartition. topic (int partition))
                                   (OffsetAndMetadata. (long off))]))
                           commit-req)]
      (.commitSync consumer commit-map))))

(defn start-consumer-loop!
  "Start a background Kafka poll loop that feeds a bounded blocking queue.

  Backpressure:
  - if the queue is full, pause assigned partitions and block on put.

  Returns {:consumer .. :thread .. :stop! .. :queue .. :commit! ..}." 
  [{:keys [consumer-config topic buffer-size name]}]
  (let [^KafkaConsumer consumer (KafkaConsumer. consumer-config)
        ^LinkedBlockingQueue q (LinkedBlockingQueue. (int buffer-size))
        ^LinkedBlockingQueue commit-q (LinkedBlockingQueue.)
        stop? (atom false)
        thread
        (Thread.
          (fn []
            (try
              (.subscribe consumer [topic])
              (log/info "Kafka consumer subscribed" {:name name :topic topic :buffer-size buffer-size})
              (while (not @stop?)
                ;; Drain commit requests (from worker threads) and commit in *this* thread.
                (let [merged
                      (loop [acc nil]
                        (if-let [req (.poll commit-q 0 TimeUnit/MILLISECONDS)]
                          (recur (merge-with max (or acc {}) req))
                          acc))]
                  (when merged
                    (try
                      (commit-reqs! consumer merged)
                      (catch Throwable t
                        (log/warn t "Commit failed" {:name name})))))
                (let [^ConsumerRecords recs (.poll consumer (Duration/ofMillis 250))]
                  (when (pos? (.count recs))
                    (doseq [^ConsumerRecord rec recs]
                      ;; Try a fast enqueue. If queue is full, pause and block.
                      (when-not (.offer q rec 0 TimeUnit/MILLISECONDS)
                        (try
                          (.pause consumer (.assignment consumer))
                          (catch Throwable t
                            (log/warn t "Failed to pause consumer" {:name name})))
                        (.put q rec)
                        (try
                          (.resume consumer (.assignment consumer))
                          (catch Throwable t
                            (log/warn t "Failed to resume consumer" {:name name}))))))))
              (log/info "Kafka consumer loop stopping" {:name name})
              (catch Throwable t
                (log/error t "Kafka consumer loop crashed" {:name name}))
              (finally
                (try (.close consumer) (catch Throwable _))))))
        stop-fn (fn [] (reset! stop? true))]
    (.setName thread (str "kafka-consumer-" name))
    (.setDaemon thread true)
    (.start thread)
    {:consumer consumer
     :thread thread
     :stop! stop-fn
     :queue q
     ;; commit! can be called from worker threads; it enqueues commit requests.
     :commit! (fn [records]
                (when-let [req (records->commit-req records)]
                  (.put commit-q req)))}))

(defn take!
  "Blocking take from consumer queue." 
  [^LinkedBlockingQueue q]
  (.take q))

(defn poll!
  "Poll with timeout (ms). Returns nil on timeout." 
  [^LinkedBlockingQueue q timeout-ms]
  (.poll q (long timeout-ms) TimeUnit/MILLISECONDS))
