(ns samuraipersistor.final-tracks-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jsonista.core :as json]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [samuraipersistor.final-track-contract :as contract]
            [samuraipersistor.final-tracks :as tracks]
            [samuraipersistor.final-track-consumer :as consumer]
            [samuraipersistor.persist :as persist]
            [samuraipersistor.testcontainers :as tc])
  (:import (java.util Base64 UUID)
           (org.apache.kafka.clients.consumer ConsumerRecord)
           (samuraibff.proto SessionTranscript)))

(defn fixture
  "Read the synthetic Python-produced cross-language contract vector."
  []
  (json/read-value (slurp (io/resource "final_track_vector.json")) contract/mapper))

(defn record
  "Build a synthetic consumer record using the actual producer header bytes."
  [primary?]
  (let [data (fixture)
        value (.decode (Base64/getDecoder) ^String (if primary? (:primary data) (:canonical data)))
        topic (if primary? "transcripts.final" "transcripts.final-tracks")
        key (.getBytes ^String (get-in data [:plan :session_id]) "UTF-8")
        record (ConsumerRecord. topic 0 0 key value)]
    (when primary?
      (doseq [[key value] (:headers data)]
        (.add (.headers record) (name key) (.getBytes ^String value "UTF-8"))))
    record))

(defn apply-sql!
  "Apply trusted test schema DDL, including the mirrored deployment addition."
  [ds resource]
  (doseq [statement (remove str/blank? (str/split (slurp (io/resource resource)) #";"))]
    (jdbc/execute! ds [statement])))

(defn prepare-db!
  "Create an isolated deployment-compatible schema and one frozen synthetic session."
  [ds]
  (apply-sql! ds "migrations/001-create-core-schema.up.sql")
  (apply-sql! ds "migrations/002-transcript-records-append-only.up.sql")
  (jdbc/execute! ds ["ALTER TABLE sessions ADD COLUMN stream_controls jsonb"])
  (apply-sql! ds "final_track_schema.sql")
  (let [plan (:plan (fixture))]
    (jdbc/execute! ds ["INSERT INTO tenants(id,name) VALUES (?,?)" (contract/uuid (:tenant_id plan)) "fixture"])
    (jdbc/execute! ds ["INSERT INTO sessions(id,tenant_id,session_key,stream_controls) VALUES (?,?,?,?::jsonb)"
                       (contract/uuid (:session_id plan)) (contract/uuid (:tenant_id plan)) (:session_id plan)
                       (json/write-value-as-string {:asr_plan plan} contract/mapper)])))

(defn counts
  "Read only row counts from the isolated fixture database."
  [ds]
  (jdbc/execute-one! ds ["SELECT (SELECT count(*) FROM recordings) AS recordings, (SELECT count(*) FROM session_transcripts) AS transcripts, (SELECT count(*) FROM transcript_track_results) AS outcomes"]
                     {:builder-fn rs/as-unqualified-lower-maps}))

(deftest python-java-identities-and-envelope
  (let [data (fixture)
        event (contract/outcome (.value (record false)) {:bucket "recordings"})]
    (is (= [(:run_id data) (:result_id data)] (contract/identities event)))
    (is (= "succeeded" (:status event)))
    (is (thrown? clojure.lang.ExceptionInfo
                 (contract/outcome (.value (record false)) {:bucket "another-bucket"})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (contract/validate-identity! (assoc event :tenant-id (str (UUID/randomUUID))) {:bucket "recordings"})))
    (let [partial (record true)]
      (.remove (.headers partial) "x-source-artifact-id")
      (is (thrown? clojure.lang.ExceptionInfo (contract/projection-headers partial))))))

(deftest missing-canonical-key-is-a-permanent-contract-error
  (let [original (record false)
        missing-key (ConsumerRecord. (.topic original) 0 0 nil (.value original))]
    (is (thrown? clojure.lang.ExceptionInfo
                 (consumer/persist-record! nil {:final-source-bucket "recordings"} false missing-key)))))

(deftest independent-arrival-order-and-idempotency
  (let [pg (tc/start-postgres!)]
    (try
      (let [ds (jdbc/get-datasource {:jdbcUrl (.getJdbcUrl pg) :user "drsynth" :password "drsynth"})
            config {:final-source-bucket "recordings"}
            canonical (contract/outcome (.value (record false)) {:bucket "recordings"})]
        (prepare-db! ds)
        (doseq [order [[true false] [false true]]]
          (jdbc/execute! ds ["TRUNCATE session_transcripts, transcript_track_results, recordings CASCADE"])
          (doseq [primary? (concat order order)]
            (is (= :ok (consumer/persist-record! ds config primary? (record primary?)))))
          (is (= {:recordings 1 :transcripts 1 :outcomes 1} (counts ds)))
          (is (thrown? clojure.lang.ExceptionInfo
                       (tracks/insert-outcome! ds (assoc canonical :event-sha256 (apply str (repeat 64 "c"))))))
          (is (= {:recordings 1 :transcripts 1 :outcomes 1} (counts ds)))
          (is (= :missing-session (tracks/insert-outcome! ds (assoc canonical :tenant-id (str (UUID/randomUUID)))))))
        (let [legacy (SessionTranscript/parseFrom ^bytes (.value (record true)))]
          (is (= :ok (persist/insert-final! ds legacy {:source "legacy" :model "whisperx"})))
          (is (= 2 (:transcripts (counts ds))))))
      (finally (tc/stop! pg)))))
