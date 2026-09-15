(ns samuraipersistor.persist-test
  (:require [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [samuraipersistor.persist :as persist]
            [samuraipersistor.testcontainers :as tc]
            [samuraipersistor.webhook-outcome-consumer :as wh-oc]
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
                    UNIQUE (session_id, recording_url),
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
                    track_id text,
                    window_length integer,
                    segment_start_s double precision,
                    segment_end_s double precision,
                    supersedes_seq bigint[],
                    event_created_at_ns bigint,
                    created_at timestamptz NOT NULL DEFAULT now()
                  );"])

  (jdbc/execute! ds ["CREATE UNIQUE INDEX IF NOT EXISTS session_transcripts_recording_final_track_unique
                     ON session_transcripts (recording_id, track_id)
                     WHERE type='final' AND track_id IS NOT NULL"])

   ;; Workflows (RFC-0003) persistence tables.
   (jdbc/execute! ds
                 ["CREATE TABLE IF NOT EXISTS workflow_results_history (
                     id uuid PRIMARY KEY,
                     created_at timestamptz NOT NULL,
                     workflow_run_id uuid NOT NULL,
                     tenant_id uuid NOT NULL,
                     session_id uuid NOT NULL,
                     workflow_id uuid NOT NULL,
                     trigger_type text NULL,
                     trigger_source_event_id text NULL,
                     status text NOT NULL,
                     render_markdown text NULL,
                     render_json jsonb NULL,
                     provider_type text NULL,
                     provider_model_id text NULL,
                     usage_input_tokens integer NULL,
                     usage_output_tokens integer NULL,
                     stream_source_uri text NULL,
                     stream_source_node_id text NULL,
                     error_code text NULL,
                     error_detail text NULL,
                     kafka_topic text NULL,
                     kafka_partition integer NULL,
                     kafka_offset bigint NULL
                   );"])

   (jdbc/execute! ds
                 ["CREATE UNIQUE INDEX IF NOT EXISTS workflow_results_history_run_uniq
                   ON workflow_results_history (workflow_run_id);"])

   (jdbc/execute! ds
                 ["CREATE TABLE IF NOT EXISTS workflow_results_latest (
                     session_id uuid NOT NULL,
                     workflow_id uuid NOT NULL,
                     created_at timestamptz NOT NULL,
                     workflow_run_id uuid NOT NULL,
                     tenant_id uuid NOT NULL,
                     trigger_type text NULL,
                     trigger_source_event_id text NULL,
                     status text NOT NULL,
                     render_markdown text NULL,
                     render_json jsonb NULL,
                     provider_type text NULL,
                     provider_model_id text NULL,
                     usage_input_tokens integer NULL,
                     usage_output_tokens integer NULL,
                     stream_source_uri text NULL,
                     stream_source_node_id text NULL,
                     error_code text NULL,
                     error_detail text NULL,
                     kafka_topic text NULL,
                     kafka_partition integer NULL,
                     kafka_offset bigint NULL,
                     PRIMARY KEY (session_id, workflow_id)
                   );"])

   (jdbc/execute! ds
                 ["CREATE TABLE IF NOT EXISTS workflow_outcomes (
                     id uuid PRIMARY KEY,
                     created_at timestamptz NOT NULL,
                     workflow_run_id uuid NOT NULL,
                     tenant_id uuid NOT NULL,
                     session_id uuid NOT NULL,
                     workflow_id uuid NOT NULL,
                     attempt_no integer NOT NULL,
                     status text NOT NULL,
                     latency_ms bigint NULL,
                     retry_to_topic text NULL,
                     error_code text NULL,
                     error_detail text NULL,
                     kafka_topic text NULL,
                     kafka_partition integer NULL,
                     kafka_offset bigint NULL
                   );"])

   (jdbc/execute! ds
                 ["CREATE UNIQUE INDEX IF NOT EXISTS workflow_outcomes_run_attempt_uniq
                   ON workflow_outcomes (workflow_run_id, attempt_no);"])

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
  (jdbc/execute! ds [(str "TRUNCATE webhook_delivery_latest, webhook_delivery_outcomes, "
                        "workflow_results_latest, workflow_results_history, workflow_outcomes, "
                        "session_transcripts, recordings, sessions")]))

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
            (let [seg1 (-> (SessionTranscriptSegment/newBuilder)
                           (.setStartS 1.0)
                           (.setEndS 1.5)
                           (.setText "hi")
                           (.setSpeaker "SPEAKER_00")
                           (.build))
                  seg2 (-> (SessionTranscriptSegment/newBuilder)
                           (.setStartS 1.5)
                           (.setEndS 2.0)
                           (.setText "there")
                           (.setSpeaker "SPEAKER_01")
                           (.build))
                  ev (-> (RefinedEvent/newBuilder)
                         (.setSessionId session-key)
                         (.setStartS 1.0)
                         (.setEndS 2.0)
                         ;; legacy scalar text may be populated, but segments[] is source of truth
                         (.setText "hi there")
                         (.setSpeaker "")
                         (.setLang "en")
                         (.setWindowSec 60.0)
                         (.setSliceIndex 0)
                         (.setFlushReason "slice")
                         (.setCreatedAtNs 1)
                         (.setRefinementModel "whisperx")
                         (.addSegments seg1)
                         (.addSegments seg2)
                         (.build))]
              (is (= :ok (persist/insert-refined!
                           ds ev
                           {:source "whisperx_worker"
                             ;; let persistor take `model/window_length/event_created_at_ns` from the event itself
                             :model nil
                             :window-length nil
                             :event-created-at-ns nil})))
              (let [n (-> (jdbc/execute-one! ds ["SELECT count(*) AS n FROM session_transcripts WHERE type='refined'"])
                          :n)]
                (is (= 1 n)))

              (let [row (jdbc/execute-one!
                          ds
                          [(str "SELECT jsonb_array_length(segments) AS seg_count, "
                                "segments #>> '{0,speaker}' AS first_speaker, "
                                "segments #>> '{1,text}' AS second_text, "
                                "window_length, model, event_created_at_ns "
                                "FROM session_transcripts WHERE type='refined' ORDER BY created_at DESC LIMIT 1")]
                          {:builder-fn rs/as-unqualified-lower-maps})]
                (is (= 2 (:seg_count row)))
                (is (= "SPEAKER_00" (:first_speaker row)))
                (is (= "there" (:second_text row)))
                (is (= 60 (:window_length row)))
                (is (= "whisperx" (:model row)))
                (is (= 1 (:event_created_at_ns row))))))

          (testing "refined insert backwards compatibility (scalar fields only)"
            (let [ev (-> (RefinedEvent/newBuilder)
                         (.setSessionId session-key)
                         (.setStartS 10.0)
                         (.setEndS 11.0)
                         (.setText "legacy")
                         (.setSpeaker "")
                         (.setLang "en")
                         (.build))]
              (is (= :ok (persist/insert-refined!
                           ds ev
                           {:source "whisperx_worker"
                            :model "whisperx"
                            :window-length 60
                            :event-created-at-ns 2})))
              (let [row (jdbc/execute-one!
                          ds
                          [(str "SELECT jsonb_array_length(segments) AS seg_count, "
                                "segments #>> '{0,text}' AS first_text "
                                "FROM session_transcripts WHERE type='refined' ORDER BY created_at DESC LIMIT 1")]
                          {:builder-fn rs/as-unqualified-lower-maps})]
                (is (= 1 (:seg_count row)))
                (is (= "legacy" (:first_text row))))))

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

(deftest webhook-outcome-created-at-parsing-test
  (testing "numeric epoch seconds encoded as double/scientific notation"
    ;; we round to millis for numeric epoch seconds because doubles are imprecise
    (is (= (Instant/ofEpochMilli 1776794181330)
           (#'wh-oc/parse-instant 1.776794181330183E9))))

  (testing "numeric epoch seconds encoded as string"
    (is (= (Instant/ofEpochMilli 1776794181330)
           (#'wh-oc/parse-instant "1.776794181330183E9"))))

  (testing "epoch millis encoded as number"
    (is (= (Instant/ofEpochMilli 1710000000123)
           (#'wh-oc/parse-instant 1710000000123))))

  (testing "ISO string"
    (is (= (Instant/parse "2026-04-12T19:00:00Z")
           (#'wh-oc/parse-instant "2026-04-12T19:00:00Z")))))

(deftest workflow-result-final-vs-incremental-semantics-test
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
              session-key "sess-1"
              workflow-id (UUID/randomUUID)
              run-id-1 (UUID/randomUUID)
              run-id-2 (UUID/randomUUID)
              created-1 (Instant/parse "2026-05-01T20:00:00Z")
              created-2 (Instant/parse "2026-05-01T20:01:00Z")]
          ;; Session must exist (workflow pipeline uses sessions.id UUID)
          (jdbc/execute! ds
                        ["INSERT INTO sessions (id, tenant_id, user_id, session_key) VALUES (?, ?, NULL, ?)"
                         session-id tenant-id session-key])

          (testing "final-like trigger appends history and updates latest"
            (is (= :ok
                   (persist/insert-workflow-result!
                     ds
                     {:workflow-run-id run-id-1
                      :tenant-id tenant-id
                      :session-id session-id
                      :workflow-id workflow-id
                      :trigger-type "transcript.final.ready"
                      :trigger-source-event-id "ev-1"
                      :status "ok"
                      :render-markdown "# Result"
                      :render-json {:a 1}
                      :provider-type "bedrock"
                      :provider-model-id "claude"
                      :usage-input-tokens 10
                      :usage-output-tokens 20
                      :created-at created-1
                      :kafka {:topic "workflow.result" :partition 0 :offset 1}})))

            (let [n (-> (jdbc/execute-one! ds ["SELECT count(*) AS n FROM workflow_results_history"]) :n)]
              (is (= 1 n)))

            (let [n (-> (jdbc/execute-one! ds ["SELECT count(*) AS n FROM workflow_results_latest"]) :n)]
              (is (= 1 n))))

          (testing "incremental trigger overwrites latest only (no history)"
            (is (= :ok
                   (persist/insert-workflow-result!
                     ds
                     {:workflow-run-id run-id-2
                      :tenant-id tenant-id
                      :session-id session-id
                      :workflow-id workflow-id
                      :trigger-type "transcript.refined.segment"
                      :trigger-source-event-id "ev-2"
                      :status "ok"
                      :render-markdown "# Incremental"
                      :render-json {:b 2}
                      :provider-type "bedrock"
                      :provider-model-id "claude"
                      :created-at created-2
                      :kafka {:topic "workflow.result" :partition 0 :offset 2}})))

            (let [n (-> (jdbc/execute-one! ds ["SELECT count(*) AS n FROM workflow_results_history"]) :n)]
              (is (= 1 n) "history should still contain only the final result"))

            (let [row (jdbc/execute-one!
                        ds
                        ["SELECT workflow_run_id, trigger_type, render_markdown
                          FROM workflow_results_latest WHERE session_id=? AND workflow_id=?"
                         session-id workflow-id]
                        {:builder-fn rs/as-unqualified-lower-maps})]
              (is (= (str run-id-2) (str (:workflow_run_id row))))
              (is (= "transcript.refined.segment" (:trigger_type row)))
              (is (= "# Incremental" (:render_markdown row))))))

        (finally
          (log/info "Stopping Postgres testcontainer")
          (tc/stop! pg))))))

(deftest workflow-outcome-idempotency-test
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
              session-key "sess-1"
              workflow-id (UUID/randomUUID)
              run-id (UUID/randomUUID)
              now (Instant/parse "2026-05-01T20:00:00Z")]
          ;; Session row is not required by workflow_outcomes table (audit lane),
          ;; but we insert it anyway for realism.
          (jdbc/execute! ds
                        ["INSERT INTO sessions (id, tenant_id, user_id, session_key) VALUES (?, ?, NULL, ?)"
                         session-id tenant-id session-key])

          (is (= :ok
                 (persist/insert-workflow-outcome!
                   ds
                   {:workflow-run-id run-id
                    :tenant-id tenant-id
                    :session-id session-id
                    :workflow-id workflow-id
                    :attempt-no 0
                    :status "SUCCESS"
                    :latency-ms 5
                    :retry-to-topic nil
                    :error-code nil
                    :error-detail nil
                    :created-at now
                    :kafka {:topic "workflow.outcome" :partition 0 :offset 10}})))

          (is (= :ok
                 (persist/insert-workflow-outcome!
                   ds
                   {:workflow-run-id run-id
                    :tenant-id tenant-id
                    :session-id session-id
                    :workflow-id workflow-id
                    :attempt-no 0
                    :status "SUCCESS"
                    :created-at now
                    :kafka {:topic "workflow.outcome" :partition 0 :offset 11}})))

          (let [n (-> (jdbc/execute-one! ds ["SELECT count(*) AS n FROM workflow_outcomes"]) :n)]
            (is (= 1 n))))

        (finally
          (log/info "Stopping Postgres testcontainer")
          (tc/stop! pg))))))

(deftest concurrent-final-tracks-and-replays-test
  (let [pg (tc/start-postgres!)
        ds (jdbc/get-datasource {:jdbcUrl (.getJdbcUrl pg) :user "drsynth" :password "drsynth"})]
    (try
      (create-minimal-schema! ds)
      (let [tenant (UUID/randomUUID)
            session (UUID/randomUUID)
            _ (jdbc/execute! ds ["INSERT INTO sessions(id,tenant_id,session_key) VALUES (?,?,?)"
                                 session tenant (str session)])
            event (-> (SessionTranscript/newBuilder)
                      (.setSessionId (str session)) (.setTenantId (str tenant))
                      (.setRecordingUrl "file://fixture.wav") (.setFullText "first")
                      (.setTrackId "whisperx") (.build))
            shadow (-> (.toBuilder event) (.setTrackId "test-shadow") (.build))
            results (mapv (fn [ev] (future (persist/insert-final! ds ev {:model "medium"})))
                          [event shadow event shadow])]
        (is (= [:ok :ok :ok :ok] (mapv deref results)))
        (is (= :ok (persist/insert-final! ds (-> (.toBuilder event) (.setFullText "replay") (.build)) {})))
        (is (= :tenant-mismatch
               (persist/insert-final! ds (-> (.toBuilder event) (.setTenantId (str (UUID/randomUUID))) (.build)) {})))
        (is (= 1 (:n (jdbc/execute-one! ds ["SELECT count(*) AS n FROM recordings"]))))
        (is (= 2 (:n (jdbc/execute-one! ds ["SELECT count(*) AS n FROM session_transcripts"]))))
        (is (= ["first" "first"]
               (mapv :session_transcripts/full_text (jdbc/execute! ds ["SELECT full_text FROM session_transcripts"]))))
        (is (= ["medium" "medium"]
               (mapv :session_transcripts/model (jdbc/execute! ds ["SELECT model FROM session_transcripts"])))))
      (finally (tc/stop! pg)))))
