(ns samuraipersistor.persist
  (:require [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [org.corfield.logging4j2 :as log]
            [jsonista.core :as j])
  (:import (java.util UUID)
           (samuraibff.proto RefinedEvent SessionTranscript SessionTranscriptSegment)))

(def ^:private json-writer
  (j/object-mapper {:encode-key-fn name}))

(defn session-uuid-by-key
  "Lookup DB session UUID (`sessions.id`) by business key (`sessions.session_key`).

  Returns nil when missing." 
  [ds session-key]
  (jdbc/execute-one! ds
                    ["SELECT id, tenant_id, user_id FROM sessions WHERE session_key=?" session-key]
                    {:builder-fn rs/as-unqualified-lower-maps}))

(defn insert-refined!
  "Persist a RefinedEvent as an append-only transcript record.

  `meta` must contain:
  - :window-length (int)
  - :model (string)
  - :source (string)  ; originating worker name, e.g. 'whisperx_worker'
  - :event-created-at-ns (long)

  Returns :ok, :missing-session, or throws on DB errors." 
  [ds ^RefinedEvent ev {:keys [window-length model source event-created-at-ns]}]
  (let [session-key (.getSessionId ev)
        row (session-uuid-by-key ds session-key)]
    (if-not row
      (do
        (log/warn "Missing session for refined event" {:session-key session-key})
        :missing-session)
      (let [{:keys [id tenant_id user_id]} row
            segment {:start_s (.getStartS ev)
                     :end_s (.getEndS ev)
                     :text (.getText ev)
                     :speaker (.getSpeaker ev)}
            segments-json (j/write-value-as-string [segment] json-writer)
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
                 (.getText ev)
                 (let [lang (.getLang ev)] (when (seq lang) lang))
                 segments-json
                 (or source "unknown")
                 (or model "unknown")
                 (when window-length (long window-length))
                 (double (.getStartS ev))
                 (double (.getEndS ev))
                 sup-arr
                 (when event-created-at-ns (long event-created-at-ns))]))
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
            segments (map (fn [^SessionTranscriptSegment s]
                            {:start_s (.getStartS s)
                             :end_s (.getEndS s)
                             :text (.getText s)
                             :speaker (.getSpeaker s)})
                          (.getSegmentsList ev))
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
