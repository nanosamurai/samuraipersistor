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
