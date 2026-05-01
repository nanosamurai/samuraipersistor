(ns samuraipersistor.persist
  (:require [clojure.string :as str]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [org.corfield.logging4j2 :as log]
            [jsonista.core :as j])
  (:import (java.sql Timestamp)
           (java.time Instant)
           (java.util UUID)
           (samuraibff.proto RefinedEvent SessionTranscript SessionTranscriptSegment WordAlignment)))

(def ^:private json-writer
  (j/object-mapper {:encode-key-fn name}))

(defn session-uuid-by-key
  "Lookup DB session UUID (`sessions.id`) by business key (`sessions.session_key`).

  Returns nil when missing." 
  [ds session-key]
  (jdbc/execute-one! ds
                    ["SELECT id, tenant_id, user_id FROM sessions WHERE session_key=?" session-key]
                    {:builder-fn rs/as-unqualified-lower-maps}))

(defn session-by-id
  "Lookup DB session row by primary key (`sessions.id`).

  Returns a map {:id uuid :tenant_id uuid :user_id uuid?} or nil when missing." 
  [ds ^UUID session-id]
  (jdbc/execute-one! ds
                    ["SELECT id, tenant_id, user_id FROM sessions WHERE id=?" session-id]
                    {:builder-fn rs/as-unqualified-lower-maps}))

(defn- words->vec
  "Convert a protobuf `repeated WordAlignment` to a vector of maps.

  Returns an empty vector when no words are present." 
  [words-list]
  (mapv (fn [^WordAlignment w]
          {:start_s (.getStartS w)
           :end_s (.getEndS w)
           :text (.getText w)})
        words-list))

(defn- segment->map
  "Convert `SessionTranscriptSegment` protobuf message to a JSON-ready map.

  Includes `:words` only when present (non-empty)." 
  [^SessionTranscriptSegment s]
  (let [base {:start_s (.getStartS s)
              :end_s (.getEndS s)
              :text (.getText s)
              :speaker (.getSpeaker s)}
        words (words->vec (.getWordsList s))]
    (cond-> base
      (seq words) (assoc :words words))))

(defn- refined-legacy->segments
  "Backward-compatible refined event segments.

  For older producers that don't populate `RefinedEvent.segments`, we treat the
  scalar fields on the event itself as a single segment." 
  [^RefinedEvent ev]
  [{:start_s (.getStartS ev)
    :end_s (.getEndS ev)
    :text (.getText ev)
    :speaker (.getSpeaker ev)}])

(defn- refined-event->segments
  "Extract refined segments from the event.

  Uses `ev.segments` when present; otherwise falls back to the legacy scalar
  fields.

  Returns a seq of maps." 
  [^RefinedEvent ev]
  (if (pos? (.getSegmentsCount ev))
    (map segment->map (.getSegmentsList ev))
    (refined-legacy->segments ev)))

(defn- segments->full-text
  "Derive `full_text` from segments.

  Join strategy is intentionally simple: concatenate segment texts with a
  single space." 
  [segments]
  (->> segments
       (keep :text)
       (remove str/blank?)
       (str/join " ")))

(defn insert-refined!
  "Persist a RefinedEvent as an append-only transcript record.

  `meta` keys (all optional):
  - :window-length (int)
  - :model (string)
  - :source (string)  ; originating worker name, e.g. 'whisperx_worker'
  - :event-created-at-ns (long)

  When `meta` doesn't contain some values, we try to fall back to new
  RefinedEvent fields (window_sec/created_at_ns/refinement_model).

  Returns :ok, :missing-session, or throws on DB errors." 
  [ds ^RefinedEvent ev {:keys [window-length model source event-created-at-ns]}]
  (let [session-key (.getSessionId ev)
        row (session-uuid-by-key ds session-key)]
    (if-not row
      (do
        (log/warn "Missing session for refined event" {:session-key session-key})
        :missing-session)
      (let [{:keys [id tenant_id user_id]} row
            segments (refined-event->segments ev)
            segments-json (j/write-value-as-string segments json-writer)
            window-length (or (when window-length (long window-length))
                              (let [ws (.getWindowSec ev)]
                                (when (pos? ws)
                                  (long (Math/round (double ws))))))
            event-created-at-ns (or (when event-created-at-ns (long event-created-at-ns))
                                    (let [t (.getCreatedAtNs ev)]
                                      (when (pos? t)
                                        (long t))))
            model (or model
                      (let [m (.getRefinementModel ev)]
                        (when (seq m) m)))
            full-text (let [t (.getText ev)]
                        (if (str/blank? t)
                          (segments->full-text segments)
                          t))
            sup (.getSupersedesSeqList ev)]
        (with-open [conn (jdbc/get-connection ds)]
          (let [sup-arr (when (and sup (pos? (.size sup)))
                          (.createArrayOf conn "int8" (into-array Long (map long sup))))]
            (jdbc/execute-one!
              conn
              (into
                ["INSERT INTO session_transcripts
            (id, session_id, recording_id, tenant_id, user_id,
             full_text, lang, duration_s, segments,
             source, type, model, window_length,
             segment_start_s, segment_end_s,
             supersedes_seq, event_created_at_ns)
          VALUES
            (?, ?, NULL, ?, ?,
             ?, ?, NULL, ?::jsonb,
             ?, 'refined', ?, ?,
             ?, ?,
             ?, ?)"]
                [(UUID/randomUUID)
                 id
                 tenant_id
                 user_id
                 full-text
                 (let [lang (.getLang ev)] (when (seq lang) lang))
                 segments-json
                 (or source "unknown")
                 (or model "unknown")
                 window-length
                 (double (.getStartS ev))
                 (double (.getEndS ev))
                 sup-arr
                 event-created-at-ns]))
            :ok))))))

(defn insert-final!
  "Persist a SessionTranscript (final transcript) + recording row + update session status.

  Expects:
  - the session exists in DB (created by BFF)
  - `recording-url`, `duration-s`, `sample-rate`, `lang` are provided in the event

  `meta` keys:
  - :source (string) e.g. 'finalizer_worker'
  - :model (string)
  - :event-created-at-ns (long)

  Returns :ok or :missing-session." 
  [ds ^SessionTranscript ev {:keys [source model event-created-at-ns]}]
  (let [session-key (.getSessionId ev)
        row (session-uuid-by-key ds session-key)]
    (if-not row
      (do
        (log/warn "Missing session for final transcript" {:session-key session-key})
        :missing-session)
      (let [{:keys [id tenant_id user_id]} row
            segments (map segment->map (.getSegmentsList ev))
            segments-json (j/write-value-as-string segments json-writer)
            recording-id (UUID/randomUUID)
            transcript-id (UUID/randomUUID)]
        (jdbc/with-transaction [tx ds]
          ;; Recording row (if the schema evolves to allow multiple recordings per session,
          ;; this stays correct; for now it’s 1 per finalization run).
          (jdbc/execute-one!
            tx
            (into
              ["INSERT INTO recordings (id, session_id, recording_url, duration_s, sample_rate, lang)
              VALUES (?, ?, ?, ?, ?, ?)"]
              [recording-id
               id
               (.getRecordingUrl ev)
               (double (.getDurationS ev))
               ;; sample_rate is not present in SessionTranscript proto currently.
               ;; DB column is NOT NULL, so we store the system default.
               16000
               (let [lang (.getLang ev)] (when (seq lang) lang))]))

          (jdbc/execute-one!
            tx
            (into
              ["INSERT INTO session_transcripts
                (id, session_id, recording_id, tenant_id, user_id,
                 full_text, lang, duration_s, segments,
                 source, type, model, window_length,
                 segment_start_s, segment_end_s,
                 supersedes_seq, event_created_at_ns)
              VALUES
                (?, ?, ?, ?, ?,
                 ?, ?, ?, ?::jsonb,
                 ?, 'final', ?, NULL,
                 NULL, NULL,
                 NULL, ?)"]
              [transcript-id
               id
               recording-id
               tenant_id
               user_id
               (.getFullText ev)
               (let [lang (.getLang ev)] (when (seq lang) lang))
               (double (.getDurationS ev))
               segments-json
               (or source "unknown")
               (or model "unknown")
               (when event-created-at-ns (long event-created-at-ns))]))

          (jdbc/execute-one!
            tx
            ["UPDATE sessions SET status='finished', ended_at=now() WHERE id=?" id]))
        :ok))))

(def ^:private webhook-error-detail-max-len
  4096)

(defn- truncate-error-detail [s]
  (when s
    (let [s (str s)]
      (if (<= (count s) webhook-error-detail-max-len)
        s
        (subs s 0 webhook-error-detail-max-len)))))

(defn parse-instant
  "Parse a timestamp that can arrive either as:

  - java.time.Instant
  - ISO-8601 string
  - epoch seconds (number or string; may be scientific notation)
  - epoch millis (number or string)

  Returns Instant or nil.

  NOTE: For numeric epoch seconds we round to millis to avoid double precision issues." 
  [x]
  (letfn [(epoch-seconds-double->instant ^Instant [^double d]
            (Instant/ofEpochMilli (long (Math/round (* d 1000.0)))))

          (number->instant [n]
            (let [d (double n)
                  epoch-ms? (> d 1.0e12)]
              (if epoch-ms?
                (Instant/ofEpochMilli (long d))
                (epoch-seconds-double->instant d))))

          (bigdec->instant [^java.math.BigDecimal bd]
            (let [epoch-ms? (> (.doubleValue bd) 1.0e12)]
              (if epoch-ms?
                (Instant/ofEpochMilli (.longValue bd))
                (let [ms (.longValue (.setScale (.multiply bd (java.math.BigDecimal/valueOf 1000))
                                            0
                                            java.math.RoundingMode/HALF_UP))]
                  (Instant/ofEpochMilli ms)))))

          (string->instant [s]
            (let [s (str/trim s)]
              (cond
                (str/blank? s) nil
                :else
                (try
                  (bigdec->instant (java.math.BigDecimal. s))
                  (catch NumberFormatException _
                    (Instant/parse s))))))]

    (cond
      (nil? x) nil
      (instance? Instant x) x
      (number? x) (number->instant x)
      :else (string->instant (str x)))))

(defn- incremental-trigger?
  "Return true if a workflow trigger type represents an incremental workflow.

  In v1 we infer this purely from trigger.type (per RFC-0003):
  - transcript.refined.* => incremental
  - anything else => non-incremental/final" 
  [trigger-type]
  (let [t (str trigger-type)]
    (str/starts-with? t "transcript.refined")))

(defn insert-workflow-result!
  "Persist a workflow-runner `workflow.result` JSON envelope.

  Inputs:
  - ds: next.jdbc datasource
  - result: map with keys (all strings unless noted):
    {:workflow-run-id uuid
     :tenant-id uuid
     :session-id uuid
     :workflow-id uuid
     :trigger-type string?
     :trigger-source-event-id string?
     :status string
     :render-markdown string?
     :render-json any?      ; stored as jsonb
     :provider-type string?
     :provider-model-id string?
     :usage-input-tokens int?
     :usage-output-tokens int?
     :stream-source-uri string?
     :stream-source-node-id string?
     :error-code string?
     :error-detail string?
     :created-at Instant
     :kafka {:topic string :partition int :offset long}}

  Semantics:
  - incremental triggers: overwrite only `workflow_results_latest`
  - non-incremental triggers: append to `workflow_results_history` + update `workflow_results_latest`

  Returns :ok, :missing-session.
  Throws on DB errors." 
  [ds {:keys [workflow-run-id tenant-id session-id workflow-id trigger-type trigger-source-event-id
              status render-markdown render-json provider-type provider-model-id
              usage-input-tokens usage-output-tokens stream-source-uri stream-source-node-id
              error-code error-detail created-at kafka]}]
  (let [{:keys [topic partition offset]} kafka
        created-at (or created-at (Instant/now))
        error-detail (truncate-error-detail error-detail)
        session-row (session-by-id ds session-id)]
    (if-not session-row
      (do
        (log/warn "Missing session for workflow result" {:session-id session-id
                                                         :workflow-id workflow-id
                                                         :workflow-run-id workflow-run-id})
        :missing-session)
      (jdbc/with-transaction [tx ds]
        (let [inc? (incremental-trigger? trigger-type)
              render-json-str (when (some? render-json)
                                (j/write-value-as-string render-json json-writer))]
          ;; Non-incremental => append history.
          (when-not inc?
            (jdbc/execute-one!
              tx
              (into
                [(str "INSERT INTO workflow_results_history\n"
                      "  (id, created_at, workflow_run_id, tenant_id, session_id, workflow_id,\n"
                      "   trigger_type, trigger_source_event_id, status,\n"
                      "   render_markdown, render_json,\n"
                      "   provider_type, provider_model_id,\n"
                      "   usage_input_tokens, usage_output_tokens,\n"
                      "   stream_source_uri, stream_source_node_id,\n"
                      "   error_code, error_detail,\n"
                      "   kafka_topic, kafka_partition, kafka_offset)\n"
                      "VALUES\n"
                      "  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n"
                      "ON CONFLICT (workflow_run_id) DO NOTHING")]
                [(UUID/randomUUID)
                 (Timestamp/from created-at)
                 workflow-run-id
                 tenant-id
                 session-id
                 workflow-id
                 trigger-type
                 trigger-source-event-id
                 status
                 render-markdown
                 render-json-str
                 provider-type
                 provider-model-id
                 (when usage-input-tokens (int usage-input-tokens))
                 (when usage-output-tokens (int usage-output-tokens))
                 stream-source-uri
                 stream-source-node-id
                 error-code
                 error-detail
                 topic
                 (when partition (int partition))
                 (when offset (long offset))])))

          ;; Always upsert latest, but only overwrite if incoming is newer.
          (jdbc/execute-one!
            tx
            (into
              [(str "INSERT INTO workflow_results_latest\n"
                    "  (session_id, workflow_id, created_at, workflow_run_id, tenant_id,\n"
                    "   trigger_type, trigger_source_event_id, status,\n"
                    "   render_markdown, render_json,\n"
                    "   provider_type, provider_model_id,\n"
                    "   usage_input_tokens, usage_output_tokens,\n"
                    "   stream_source_uri, stream_source_node_id,\n"
                    "   error_code, error_detail,\n"
                    "   kafka_topic, kafka_partition, kafka_offset)\n"
                    "VALUES\n"
                    "  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n"
                    "ON CONFLICT (session_id, workflow_id) DO UPDATE SET\n"
                    "  created_at=EXCLUDED.created_at,\n"
                    "  workflow_run_id=EXCLUDED.workflow_run_id,\n"
                    "  tenant_id=EXCLUDED.tenant_id,\n"
                    "  trigger_type=EXCLUDED.trigger_type,\n"
                    "  trigger_source_event_id=EXCLUDED.trigger_source_event_id,\n"
                    "  status=EXCLUDED.status,\n"
                    "  render_markdown=EXCLUDED.render_markdown,\n"
                    "  render_json=EXCLUDED.render_json,\n"
                    "  provider_type=EXCLUDED.provider_type,\n"
                    "  provider_model_id=EXCLUDED.provider_model_id,\n"
                    "  usage_input_tokens=EXCLUDED.usage_input_tokens,\n"
                    "  usage_output_tokens=EXCLUDED.usage_output_tokens,\n"
                    "  stream_source_uri=EXCLUDED.stream_source_uri,\n"
                    "  stream_source_node_id=EXCLUDED.stream_source_node_id,\n"
                    "  error_code=EXCLUDED.error_code,\n"
                    "  error_detail=EXCLUDED.error_detail,\n"
                    "  kafka_topic=EXCLUDED.kafka_topic,\n"
                    "  kafka_partition=EXCLUDED.kafka_partition,\n"
                    "  kafka_offset=EXCLUDED.kafka_offset\n"
                    "WHERE workflow_results_latest.created_at <= EXCLUDED.created_at")]
              [session-id
               workflow-id
               (Timestamp/from created-at)
               workflow-run-id
               tenant-id
               trigger-type
               trigger-source-event-id
               status
               render-markdown
               render-json-str
               provider-type
               provider-model-id
               (when usage-input-tokens (int usage-input-tokens))
               (when usage-output-tokens (int usage-output-tokens))
               stream-source-uri
               stream-source-node-id
               error-code
               error-detail
               topic
               (when partition (int partition))
               (when offset (long offset))]))

          :ok)))))

(defn insert-workflow-outcome!
  "Persist a workflow-runner `workflow.outcome` JSON envelope.

  Inputs:
  - ds: next.jdbc datasource
  - outcome: map with keys:
    {:workflow-run-id uuid
     :tenant-id uuid
     :session-id uuid
     :workflow-id uuid
     :attempt-no int
     :status string
     :latency-ms long?
     :retry-to-topic string?
     :error-code string?
     :error-detail string?
     :created-at Instant
     :kafka {:topic string :partition int :offset long}}

  Behavior:
  - append-only insert into `workflow_outcomes` (idempotent on (workflow_run_id, attempt_no))

  Returns :ok.
  Throws on DB errors." 
  [ds {:keys [workflow-run-id tenant-id session-id workflow-id attempt-no status
              latency-ms retry-to-topic error-code error-detail created-at kafka]}]
  (let [{:keys [topic partition offset]} kafka
        created-at (or created-at (Instant/now))
        error-detail (truncate-error-detail error-detail)
        id (UUID/randomUUID)]
    (jdbc/execute-one!
      ds
      (into
        [(str "INSERT INTO workflow_outcomes\n"
              "  (id, created_at, workflow_run_id, tenant_id, session_id, workflow_id,\n"
              "   attempt_no, status, latency_ms, retry_to_topic, error_code, error_detail,\n"
              "   kafka_topic, kafka_partition, kafka_offset)\n"
              "VALUES\n"
              "  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n"
              "ON CONFLICT (workflow_run_id, attempt_no) DO NOTHING")]
        [id
         (Timestamp/from created-at)
         workflow-run-id
         tenant-id
         session-id
         workflow-id
         (int attempt-no)
         status
         (when latency-ms (long latency-ms))
         retry-to-topic
         error-code
         error-detail
         topic
         (when partition (int partition))
         (when offset (long offset))]))
    :ok))

(defn insert-webhook-delivery-outcome!
  "Persist a webhook dispatcher delivery outcome.

  Inputs:
  - ds: next.jdbc datasource
  - outcome: map with keys:
    {:dispatch-id uuid
     :event-id string?
     :event-type string
     :tenant-id uuid
     :session-id uuid?
     :webhook-id string
     :attempt-no int
     :status string
     :http-status int?
     :error-code string?
     :error-detail string?
     :latency-ms long?
     :created-at java.time.Instant
     :kafka {:topic string :partition int :offset long}}

  Behavior:
  - append-only insert into `webhook_delivery_outcomes` (idempotent on (dispatch_id, attempt_no))
  - upsert last status into `webhook_delivery_latest` (only if newer)

  Returns:
  - :ok (always, even when history row was a duplicate)
  Throws on DB errors."
  [ds {:keys [dispatch-id event-id event-type tenant-id session-id webhook-id attempt-no status
              http-status error-code error-detail latency-ms created-at kafka]}]
  (let [{:keys [topic partition offset]} kafka
        created-at (or created-at (Instant/now))
        error-detail (truncate-error-detail error-detail)
        id (UUID/randomUUID)]
    (jdbc/with-transaction [tx ds]
      ;; History (append-only)
      (jdbc/execute-one!
        tx
        (into
          [(str "INSERT INTO webhook_delivery_outcomes\n"
                "  (id, created_at, tenant_id, session_id, webhook_id, dispatch_id, event_id, event_type,\n"
                "   attempt_no, status, http_status, error_code, error_detail, latency_ms,\n"
                "   kafka_topic, kafka_partition, kafka_offset)\n"
                "VALUES\n"
                "  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n"
                "ON CONFLICT (dispatch_id, attempt_no) DO NOTHING")]
          [id
           (Timestamp/from created-at)
           tenant-id
           session-id
           webhook-id
           dispatch-id
           event-id
           event-type
           (int attempt-no)
           status
           http-status
           error-code
           error-detail
           (when latency-ms (long latency-ms))
           topic
           (when partition (int partition))
           (when offset (long offset))]))

      ;; Latest (fast list view) - update only if this outcome is newer.
      (jdbc/execute-one!
        tx
        (into
          [(str "INSERT INTO webhook_delivery_latest\n"
                "  (tenant_id, webhook_id, last_created_at, last_status, last_http_status,\n"
                "   last_error_code, last_error_detail, last_latency_ms, last_event_type,\n"
                "   last_dispatch_id, last_attempt_no)\n"
                "VALUES\n"
                "  (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\n"
                "ON CONFLICT (tenant_id, webhook_id) DO UPDATE SET\n"
                "  last_created_at=EXCLUDED.last_created_at,\n"
                "  last_status=EXCLUDED.last_status,\n"
                "  last_http_status=EXCLUDED.last_http_status,\n"
                "  last_error_code=EXCLUDED.last_error_code,\n"
                "  last_error_detail=EXCLUDED.last_error_detail,\n"
                "  last_latency_ms=EXCLUDED.last_latency_ms,\n"
                "  last_event_type=EXCLUDED.last_event_type,\n"
                "  last_dispatch_id=EXCLUDED.last_dispatch_id,\n"
                "  last_attempt_no=EXCLUDED.last_attempt_no\n"
                "WHERE webhook_delivery_latest.last_created_at <= EXCLUDED.last_created_at")]
          [tenant-id
           webhook-id
           (Timestamp/from created-at)
           status
           http-status
           error-code
           error-detail
           (when latency-ms (long latency-ms))
           event-type
           dispatch-id
           (int attempt-no)]))
      :ok)))
