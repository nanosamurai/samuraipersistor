(ns samuraipersistor.persist-test
  (:require [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [samuraipersistor.persist :as persist]
            [samuraipersistor.testcontainers :as tc]
            [org.corfield.logging4j2 :as log])
  (:import (samuraibff.proto RefinedEvent SessionTranscript SessionTranscriptSegment WordAlignment)
           (java.time Instant)
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
                  );"])

  (jdbc/execute! ds
                ["CREATE TABLE IF NOT EXISTS webhook_delivery_outcomes (
                    id uuid PRIMARY KEY,
                    created_at timestamptz NOT NULL,
                    tenant_id uuid NOT NULL,
                    session_id uuid NULL,
                    webhook_id text NOT NULL,
                    dispatch_id uuid NOT NULL,
                    event_id text NULL,
                    event_type text NOT NULL,
                    attempt_no integer NOT NULL,
                    status text NOT NULL,
                    http_status integer NULL,
                    error_code text NULL,
                    error_detail text NULL,
                    latency_ms bigint NULL,
                    kafka_topic text NULL,
                    kafka_partition integer NULL,
                    kafka_offset bigint NULL
                  );"])

  (jdbc/execute! ds
                ["CREATE UNIQUE INDEX IF NOT EXISTS webhook_delivery_outcomes_dispatch_attempt_uniq
                  ON webhook_delivery_outcomes (dispatch_id, attempt_no);"])

  (jdbc/execute! ds
                ["CREATE TABLE IF NOT EXISTS webhook_delivery_latest (
                    tenant_id uuid NOT NULL,
                    webhook_id text NOT NULL,
                    last_created_at timestamptz NOT NULL,
                    last_status text NOT NULL,
                    last_http_status integer NULL,
                    last_error_code text NULL,
                    last_error_detail text NULL,
                    last_latency_ms bigint NULL,
                    last_event_type text NULL,
                    last_dispatch_id uuid NULL,
                    last_attempt_no integer NULL,
                    PRIMARY KEY (tenant_id, webhook_id)
                  );"]))

(defn- clean-db! [ds]
  (jdbc/execute! ds ["TRUNCATE webhook_delivery_latest, webhook_delivery_outcomes, session_transcripts, recordings, sessions"]))

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

(deftest webhook-delivery-outcome-insert-test
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
              dispatch-id (UUID/randomUUID)
              now (Instant/parse "2026-04-12T19:00:00Z")]
          ;; Insert twice; second should be deduped by (dispatch_id, attempt_no)
          (is (= :ok (persist/insert-webhook-delivery-outcome!
                     ds
                     {:dispatch-id dispatch-id
                      :event-id "ev_1"
                      :event-type "transcript.final.ready"
                      :tenant-id tenant-id
                      :session-id session-id
                      :webhook-id "wh_1"
                      :attempt-no 0
                      :status "SUCCESS"
                      :http-status 204
                      :error-code nil
                      :error-detail nil
                      :latency-ms 12
                      :created-at now
                      :kafka {:topic "webhook.delivery_outcome" :partition 0 :offset 10}})))

          (is (= :ok (persist/insert-webhook-delivery-outcome!
                     ds
                     {:dispatch-id dispatch-id
                      :event-id "ev_1"
                      :event-type "transcript.final.ready"
                      :tenant-id tenant-id
                      :session-id session-id
                      :webhook-id "wh_1"
                      :attempt-no 0
                      :status "SUCCESS"
                      :http-status 204
                      :latency-ms 12
                      :created-at now
                      :kafka {:topic "webhook.delivery_outcome" :partition 0 :offset 11}})))

          (let [n (-> (jdbc/execute-one! ds ["SELECT count(*) AS n FROM webhook_delivery_outcomes"])
                      :n)]
            (is (= 1 n)))

          (let [row (jdbc/execute-one!
                      ds
                      ["SELECT tenant_id, webhook_id, last_status, last_http_status, last_created_at
                        FROM webhook_delivery_latest WHERE tenant_id=? AND webhook_id=?"
                       tenant-id "wh_1"]
                      {:builder-fn rs/as-unqualified-lower-maps})]
            (is (= (str tenant-id) (str (:tenant_id row))))
            (is (= "wh_1" (:webhook_id row)))
            (is (= "SUCCESS" (:last_status row)))
            (is (= 204 (:last_http_status row)))))

        (finally
          (log/info "Stopping Postgres testcontainer")
          (tc/stop! pg))))))
