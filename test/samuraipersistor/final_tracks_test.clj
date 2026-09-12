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
           (samuraibff.proto SessionTranscript FinalTrackResult)))

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
  ([ds] (prepare-db! ds true))
  ([ds inline?]
   (apply-sql! ds "migrations/001-create-core-schema.up.sql")
   (apply-sql! ds "migrations/002-transcript-records-append-only.up.sql")
   (jdbc/execute! ds ["ALTER TABLE sessions ADD COLUMN stream_controls jsonb"])
   (apply-sql! ds "final_track_schema.sql")
   (when inline? (apply-sql! ds "inline_final_track_schema.sql"))
   (let [plan (:plan (fixture))]
     (jdbc/execute! ds ["INSERT INTO tenants(id,name) VALUES (?,?)" (contract/uuid (:tenant_id plan)) "fixture"])
     (jdbc/execute! ds ["INSERT INTO sessions(id,tenant_id,session_key,stream_controls) VALUES (?,?,?,?::jsonb)"
                        (contract/uuid (:session_id plan)) (contract/uuid (:tenant_id plan)) (:session_id plan)
                        (json/write-value-as-string {:asr_plan plan} contract/mapper)]))))

(defn counts
  "Read only row counts from the isolated fixture database."
  [ds]
  (jdbc/execute-one! ds ["SELECT (SELECT count(*) FROM recordings) AS recordings, (SELECT count(*) FROM session_transcripts) AS transcripts, (SELECT count(*) FROM session_transcripts WHERE track_id IS NOT NULL) AS outcomes"]
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
      (.remove (.headers partial) "x-result-id")
      (is (thrown? clojure.lang.ExceptionInfo (contract/projection-headers partial))))))

(deftest missing-canonical-key-is-a-permanent-contract-error
  (let [original (record false)
        missing-key (ConsumerRecord. (.topic original) 0 0 nil (.value original))]
    (is (thrown? clojure.lang.ExceptionInfo
                 (consumer/persist-record! nil {:final-source-bucket "recordings"} false missing-key nil)))))

(deftest accept-first-outcome-and-recover-publication
  (let [pg (tc/start-postgres!)
        ds (jdbc/get-datasource {:jdbcUrl (.getJdbcUrl pg) :user "drsynth" :password "drsynth"})
        config {:final-source-bucket "recordings" :topics {:final "transcripts.final"}}
        canonical (contract/outcome (.value (record false)) {:bucket "recordings"})
        delivered (atom [])]
    (try
      (prepare-db! ds)
      (is (thrown? clojure.lang.ExceptionInfo
                   (consumer/persist-record! ds config true (record true) nil)))
      (is (= {:recordings 0 :transcripts 0 :outcomes 0} (counts ds)))
      (with-redefs [consumer/publish-primary! (fn [& _] (throw (ex-info "delivery failed" {})))]
        (is (thrown? clojure.lang.ExceptionInfo
                     (consumer/persist-record! ds config false (record false) nil))))
      (is (= {:recordings 1 :transcripts 1 :outcomes 1} (counts ds)))
      (is (= "fixture" (:full_text (tracks/insert-outcome! ds (assoc canonical :full-text "losing replay")))))
      (with-redefs [consumer/publish-primary! (fn [_ _ row _]
                                                (swap! delivered conj (tracks/primary-projection row)))]
        (dotimes [_ 2] (consumer/persist-record! ds config false (record false) nil)))
      (is (= 2 (count @delivered)))
      (is (apply = @delivered))
      (is (= "fixture" (.getFullText (first @delivered))))
      (dotimes [_ 2]
        (is (= :ok (consumer/persist-record! ds config true (record true) nil))))
      (is (thrown? clojure.lang.ExceptionInfo
                   (tracks/insert-outcome! ds (assoc canonical :source-sha256 (apply str (repeat 64 "c"))))))
      (is (thrown? clojure.lang.ExceptionInfo
                   (tracks/insert-outcome! ds (assoc canonical :profile-id "conflict"))))
      (is (= {:recordings 1 :transcripts 1 :outcomes 1} (counts ds)))
      (is (= :missing-session (tracks/insert-outcome! ds (assoc canonical :tenant-id (str (UUID/randomUUID))))))
      (is (= :ok (persist/insert-final! ds (SessionTranscript/parseFrom ^bytes (.value (record true)))
                                        {:source "legacy" :model "whisperx" :event-created-at-ns 1})))
      (is (= 2 (:transcripts (counts ds))))
      (finally (tc/stop! pg)))))

(deftest old-envelope-is-held-for-explicit-conversion
  (let [old (.decode (Base64/getDecoder) ^String (:legacy_canonical (fixture)))]
    (try
      (contract/outcome old {:bucket "recordings"})
      (is false "old reference payload must not be silently accepted or discarded")
      (catch clojure.lang.ExceptionInfo error
        (is (= :samuraipersistor.final-track-contract/legacy-contract (:type (ex-data error))))))))

(deftest forward-migration-preserves-primary-and-retained-index
  (let [pg (tc/start-postgres!)
        ds (jdbc/get-datasource {:jdbcUrl (.getJdbcUrl pg) :user "drsynth" :password "drsynth"})
        data (fixture)
        plan (:plan data)
        result-id (contract/uuid (:result_id data))
        options {:builder-fn rs/as-unqualified-lower-maps}]
    (try
      (prepare-db! ds false)
      (apply-sql! ds "retained_refinement_schema.sql")
      (is (= :ok (persist/insert-final! ds (SessionTranscript/parseFrom ^bytes (.value (record true)))
                                        {:source "legacy" :model "whisperx" :event-created-at-ns 1})))
      (jdbc/execute! ds ["UPDATE session_transcripts SET result_id=?" result-id])
      (jdbc/execute! ds
                     ["INSERT INTO transcript_track_results (result_id,tenant_id,session_id,recording_id,plan_id,track_id,profile_id,run_id,attempt_id,stage,unit_id,revision,status,is_primary,capabilities,degradations,provenance,event_sha256,event_created_at_ns) SELECT result_id,tenant_id,session_id,recording_id,?,'whisperx','whisperx-final-r1',?,?,'final','recording',1,'succeeded',true,'{}','[]','{}','retained',event_created_at_ns FROM session_transcripts"
                      (contract/uuid (:plan_id plan)) (UUID/randomUUID) (UUID/randomUUID)])
      (jdbc/execute! ds
                     ["INSERT INTO transcript_track_results SELECT ?,tenant_id,session_id,NULL,plan_id,track_id,profile_id,?,attempt_id,'refined','fixed-0:1:1',revision,status,false,result_uri,result_sha256,error_code,capabilities,degradations,provenance,event_sha256,event_created_at_ns,created_at,'{}' FROM transcript_track_results"
                      (UUID/randomUUID) (UUID/randomUUID)])
      (let [before (jdbc/execute-one! ds ["SELECT full_text,segments FROM session_transcripts"] options)
            retained (jdbc/execute! ds ["SELECT * FROM transcript_track_results ORDER BY result_id"] options)]
        (apply-sql! ds "inline_final_track_schema.sql")
        (is (= before (jdbc/execute-one! ds ["SELECT full_text,segments FROM session_transcripts"] options)))
        (is (= retained (jdbc/execute! ds ["SELECT * FROM transcript_track_results ORDER BY result_id"] options)))
        (is (= {:result_id result-id :plan_id (contract/uuid (:plan_id plan))
                :track_id "whisperx" :profile_id "whisperx-final-r1" :status "succeeded" :is_primary true}
               (jdbc/execute-one! ds ["SELECT result_id,plan_id,track_id,profile_id,status,is_primary FROM session_transcripts"] options))))
      (finally (tc/stop! pg)))))

(deftest independent-tracks-reuse-recording-and-delete-with-session
  (let [pg (tc/start-postgres!)
        ds (jdbc/get-datasource {:jdbcUrl (.getJdbcUrl pg) :user "drsynth" :password "drsynth"})
        original (contract/outcome (.value (record false)) {:bucket "recordings"})
        plan (update (:plan (fixture)) :final_tracks conj
                     {:track_id "secondary" :profile_id "test-final-r1" :primary false})
        secondary (assoc original :track-id "secondary" :profile-id "test-final-r1")
        secondary (assoc secondary :result-id (second (contract/identities secondary)))]
    (try
      (prepare-db! ds)
      (jdbc/execute! ds ["UPDATE sessions SET stream_controls=?::jsonb"
                         (json/write-value-as-string {:asr_plan plan} contract/mapper)])
      (let [failed (tracks/insert-outcome! ds (assoc original :full-text "" :status "failed" :error-code "inference_failed"))
            other (tracks/insert-outcome! ds secondary)]
        (is (= "failed" (:status failed)))
        (is (nil? (tracks/primary-projection failed)))
        (is (nil? (tracks/primary-projection other)))
        (is (= "failed" (:status (tracks/insert-outcome! ds original))))
        (is (= {:recordings 1 :transcripts 2 :outcomes 2} (counts ds))))
      (jdbc/execute! ds ["DELETE FROM sessions WHERE id=?" (contract/uuid (:session-id original))])
      (is (= {:recordings 0 :transcripts 0 :outcomes 0} (counts ds)))
      (is (= :missing-session (tracks/insert-outcome! ds secondary)))
      (is (= :missing-session (consumer/persist-record! ds {:final-source-bucket "recordings"}
                                                        true (record true) nil)))
      (finally (tc/stop! pg)))))

(deftest silence-failure-and-shape-validation
  (let [original (FinalTrackResult/parseFrom ^bytes (.value (record false)))
        silent (.build (doto (.toBuilder original) (.clearFullText) (.clearSegments)))
        failed (.build (doto (.toBuilder silent) (.setStatus "failed") (.setErrorCode "inference_failed")))]
    (is (= "succeeded" (:status (contract/outcome (.toByteArray silent) {:bucket "recordings"}))))
    (is (= "failed" (:status (contract/outcome (.toByteArray failed) {:bucket "recordings"}))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (contract/outcome (.toByteArray (.build (doto (.toBuilder failed) (.setFullText "invalid"))))
                                   {:bucket "recordings"})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (contract/outcome (.toByteArray (.build (doto (.toBuilder silent) (.setWordTimestamps true))))
                                   {:bucket "recordings"})))
    (is (thrown? clojure.lang.ExceptionInfo
                 (contract/outcome (byte-array 900001) {:bucket "recordings"})))))
