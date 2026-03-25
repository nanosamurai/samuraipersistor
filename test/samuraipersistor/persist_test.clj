(ns samuraipersistor.persist-test
  (:require [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [samuraipersistor.persist :as persist]
            [samuraipersistor.testcontainers :as tc]
            [org.corfield.logging4j2 :as log])
  (:import (samuraibff.proto RefinedEvent SessionTranscript SessionTranscriptSegment WordAlignment)
           (java.util UUID)))

(defn- create-minimal-schema! [ds]
  ;; Minimal subset of drsynth schema for persistence tests.
  (jdbc/execute! ds
                 ["CREATE TABLE IF NOT EXISTS sessions (
                    id uuid PRIMARY KEY,
                    tenant_id uuid NOT NULL,
                    user_id uuid NULL,
                    session_key text NOT NULL UNIQUE,
                    status text NOT NULL DEFAULT 'active',
                    ended_at timestamptz NULL
                  );"])

  (jdbc/execute! ds
                 ["CREATE TABLE IF NOT EXISTS recordings (
                    id uuid PRIMARY KEY,
                    session_id uuid NOT NULL,
                    recording_url text NOT NULL,
                    duration_s double precision NOT NULL,
                    sample_rate integer NOT NULL,
                    lang text,
                    created_at timestamptz NOT NULL DEFAULT now()
                  );"])

  (jdbc/execute! ds
                 ["CREATE TABLE IF NOT EXISTS session_transcripts (
                    id uuid PRIMARY KEY,
                    session_id uuid NOT NULL,
                    recording_id uuid NULL,
                    tenant_id uuid NOT NULL,
                    user_id uuid,
                    full_text text NOT NULL,
                    lang text,
                    duration_s double precision,
                    segments jsonb NOT NULL,
                    source text NOT NULL,
                    type text NOT NULL,
                    model text,
                    window_length integer,
                    segment_start_s double precision,
                    segment_end_s double precision,
                    supersedes_seq bigint[],
                    event_created_at_ns bigint,
                    created_at timestamptz NOT NULL DEFAULT now()
                  );"]))

(defn- clean-db! [ds]
  (jdbc/execute! ds ["TRUNCATE session_transcripts, recordings, sessions"]))

(defn- try-start-postgres! []
  (try
    (log/info "Starting Postgres testcontainer" {:port 15432})
    (tc/start-postgres!)
    (catch Throwable t
      (log/warn t "Skipping test: failed to start Postgres testcontainer")
      nil)))

(deftest refined-and-final-inserts-test
  (let [pg (try-start-postgres!)
        ds (jdbc/get-datasource {:dbtype "postgresql"
                                :host "localhost"
                                :port 15432
                                :dbname "drsynth"
                                :user "drsynth"
                                :password "drsynth"})]
    (if-not pg
      (is true "skipped")
      (try
        (create-minimal-schema! ds)
        (clean-db! ds)

        (let [tenant-id (UUID/randomUUID)
              session-id (UUID/randomUUID)
              session-key "sess-1"]
          (jdbc/execute! ds
                        ["INSERT INTO sessions (id, tenant_id, user_id, session_key) VALUES (?, ?, NULL, ?)"
                         session-id tenant-id session-key])

          (testing "refined insert"
            (let [ev (-> (RefinedEvent/newBuilder)
                         (.setSessionId session-key)
                         (.setStartS 1.0)
                         (.setEndS 2.0)
                         (.setText "hi")
                         (.setSpeaker "")
                         (.setLang "en")
                         (.build))]
              (is (= :ok (persist/insert-refined!
                           ds ev
                           {:source "whisperx_worker"
                            :model "whisperx"
                            :window-length 60
                            :event-created-at-ns 1})))
              (let [n (-> (jdbc/execute-one! ds ["SELECT count(*) AS n FROM session_transcripts WHERE type='refined'"])
                          :n)]
                (is (= 1 n)))))

          (testing "final insert"
            (let [word (-> (WordAlignment/newBuilder)
                           (.setStartS 0.1)
                           (.setEndS 0.2)
                           (.setText "hello")
                           (.build))
                  seg (-> (SessionTranscriptSegment/newBuilder)
                          (.setStartS 0.0)
                          (.setEndS 3.0)
                          (.setText "hello")
                          (.setSpeaker "")
                          (.addWords word)
                          (.build))
                  ev (-> (SessionTranscript/newBuilder)
                         (.setSessionId session-key)
                         (.setRecordingUrl "file://dummy.wav")
                         (.setLang "en")
                         (.setDurationS 3.0)
                         (.setFullText "hello")
                         (.setTenantId (str tenant-id))
                         (.setCreatedAtNs 2)
                         (.addSegments seg)
                         (.build))]
              (is (= :ok (persist/insert-final!
                           ds ev
                           {:source "finalizer_worker"
                            :model "whisperx"
                            :event-created-at-ns 2})))
              (let [n (-> (jdbc/execute-one! ds ["SELECT count(*) AS n FROM session_transcripts WHERE type='final'"])
                          :n)]
                (is (= 1 n)))

              (let [row (jdbc/execute-one! ds
                                          [(str "SELECT segments #>> '{0,words,0,text}' AS first_word_text, "
                                                "jsonb_array_length(segments->0->'words') AS word_count "
                                                "FROM session_transcripts WHERE type='final' ORDER BY created_at DESC LIMIT 1")])]
                (is (= "hello" (:first_word_text row)))
                (is (= 1 (:word_count row)))))))

        (finally
          (log/info "Stopping Postgres testcontainer")
          (tc/stop! pg))))))
