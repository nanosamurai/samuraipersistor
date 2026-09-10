(ns samuraipersistor.refinement-tracks-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [jsonista.core :as json]
            [next.jdbc :as jdbc]
            [samuraipersistor.final-track-contract :as contract]
            [samuraipersistor.final-track-consumer :as consumer]
            [samuraipersistor.final-tracks-test :as fixture]
            [samuraipersistor.testcontainers :as tc])
  (:import (java.util Base64 UUID)
           (org.apache.kafka.clients.consumer ConsumerRecord)
           (samuraibff.proto FinalTrackResult)))

(defn vector-data
  "Read the synthetic two-window/two-track Python contract fixture."
  []
  (json/read-value (slurp (io/resource "refinement_track_vector.json")) contract/mapper))

(defn record
  "Build a canonical or primary record carrying the accepted outcome envelope."
  [data row primary?]
  (let [decode #(.decode (Base64/getDecoder) ^String %)
        value (decode (get row (if primary? :primary :canonical)))
        rec (ConsumerRecord. (if primary? "transcripts.refined" "transcripts.refined-tracks") 0 0
                             (.getBytes ^String (get-in data [:plan :session_id]) "UTF-8") value)]
    (when primary? (.add (.headers rec) "x-track-outcome" (decode (:canonical row))))
    rec))

(deftest python-refinement-identities-and-source-bounds
  (let [data (vector-data)
        events (mapv #(contract/outcome (.value (record data % false)) {:bucket "recordings"}) (:outcomes data))]
    (is (= 4 (count (distinct (map :result-id events)))))
    (is (= 2 (count (distinct (map :run-id events)))))
    (is (= 2 (count (distinct (map :source-id events)))))
    (doseq [event events]
      (is (= [(:run-id event) (:result-id event)] (contract/identities event))))
    (let [original (record data (first (:outcomes data)) false)
          event (FinalTrackResult/parseFrom ^bytes (.value original))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (contract/outcome (.toByteArray (.build (.setTenantId (.toBuilder event) (str (UUID/randomUUID)))))
                                     {:bucket "recordings"}))))))

(deftest persist-each-track-and-window-without-recording-rows
  (let [pg (tc/start-postgres!)]
    (try
      (let [ds (jdbc/get-datasource {:jdbcUrl (.getJdbcUrl pg) :user "drsynth" :password "drsynth"})
            data (vector-data) plan (:plan data)
            config {:final-source-bucket "recordings" :track-stage "refined"}]
        (fixture/prepare-db! ds)
        (jdbc/execute! ds ["INSERT INTO tenants(id,name) VALUES (?,?)" (contract/uuid (:tenant_id plan)) "refinement"])
        (jdbc/execute! ds ["INSERT INTO sessions(id,tenant_id,session_key,stream_controls) VALUES (?,?,?,?::jsonb)"
                           (contract/uuid (:session_id plan)) (contract/uuid (:tenant_id plan)) (:session_id plan)
                           (json/write-value-as-string {:asr_plan plan} contract/mapper)])
        (doseq [order [[true false] [false true]]]
          (jdbc/execute! ds ["TRUNCATE session_transcripts, transcript_track_results, recordings CASCADE"])
          (doseq [primary? (concat order order)
                  row (:outcomes data)
                  :when (or (not primary?) (:primary row))]
            (is (= :ok (consumer/persist-record! ds config primary? (record data row primary?)))))
          (is (= {:recordings 0 :transcripts 2 :outcomes 4} (fixture/counts ds))))
        (is (thrown? clojure.lang.ExceptionInfo
                     (consumer/persist-record! ds {:final-source-bucket "recordings"} false
                                               (record data (first (:outcomes data)) false)))))
      (finally (tc/stop! pg)))))
