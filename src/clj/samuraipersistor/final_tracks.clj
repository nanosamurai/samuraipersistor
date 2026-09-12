(ns samuraipersistor.final-tracks
  "Tenant-scoped, idempotent canonical outcomes and primary transcript projections."
  (:require [jsonista.core :as json]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [samuraipersistor.final-track-contract :as contract])
  (:import (java.util UUID)
           (org.postgresql.util PGobject)
           (samuraibff.proto SessionTranscript SessionTranscriptSegment WordAlignment)))

(defn- encode
  "Encode a JSONB parameter using the shared contract mapper."
  [value]
  (json/write-value-as-string value contract/mapper))

(defn- session!
  "Resolve an owned session and verify its frozen plan selection; nil means missing."
  [tx {:keys [tenant-id session-id plan-id track-id profile-id]}]
  (when-let [row (jdbc/execute-one! tx
                                    ["SELECT id, tenant_id, user_id, stream_controls FROM sessions WHERE id=? AND tenant_id=? FOR NO KEY UPDATE"
                                     (contract/uuid session-id) (contract/uuid tenant-id)]
                                    {:builder-fn rs/as-unqualified-lower-maps})]
    (let [raw (:stream_controls row)
          controls (if (instance? PGobject raw)
                     (json/read-value (.getValue ^PGobject raw) contract/mapper) raw)
          plan (:asr_plan controls)
          selection (some #(when (= track-id (:track_id %)) %) (:final_tracks plan))]
      (when-not (and (= plan-id (:plan_id plan)) (= tenant-id (:tenant_id plan))
                     (= session-id (:session_id plan)) (= profile-id (:profile_id selection))
                     (boolean? (:primary selection)))
        (contract/invalid! :session-plan))
      (assoc row :primary? (:primary selection)))))

(defn- recording!
  "Insert or reuse the one recording row; reject a reused ID with different bytes."
  [tx {:keys [session-id source-id source-uri source-sha256 source-size sample-count sample-rate version-id lang]}]
  (let [row (jdbc/execute-one! tx
                               ["INSERT INTO recordings (id,session_id,recording_url,duration_s,sample_rate,lang,source_artifact_id,source_sha256,source_size_bytes,source_sample_count,source_version_id) VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (session_id,source_artifact_id) DO UPDATE SET id=recordings.id WHERE recordings.recording_url=EXCLUDED.recording_url AND recordings.source_sha256=EXCLUDED.source_sha256 AND recordings.source_size_bytes=EXCLUDED.source_size_bytes AND recordings.source_sample_count=EXCLUDED.source_sample_count AND recordings.sample_rate=EXCLUDED.sample_rate AND recordings.source_version_id=EXCLUDED.source_version_id RETURNING id"
                                (UUID/randomUUID) (contract/uuid session-id) source-uri (/ sample-count (double sample-rate))
                                sample-rate lang (contract/uuid source-id) source-sha256 source-size sample-count version-id]
                               {:builder-fn rs/as-unqualified-lower-maps})]
    (or (:id row) (contract/invalid! :source-conflict))))

(defn- decode
  "Decode JDBC JSONB values to transcript data."
  [value]
  (if (instance? PGobject value)
    (json/read-value (.getValue ^PGobject value) contract/mapper) value))

(defn accepted-result
  "Read an accepted tenant-owned outcome, including source metadata, from Postgres."
  [ds tenant-id session-id result-id]
  (when-let [row (jdbc/execute-one! ds
                                    ["SELECT st.*, r.recording_url, r.source_artifact_id, r.source_sha256, r.source_size_bytes, r.source_sample_count, r.source_version_id, r.sample_rate FROM session_transcripts st JOIN recordings r ON r.id=st.recording_id WHERE st.result_id=? AND st.tenant_id=? AND st.session_id=? AND st.track_id IS NOT NULL"
                                     (contract/uuid result-id) (contract/uuid tenant-id) (contract/uuid session-id)]
                                    {:builder-fn rs/as-unqualified-lower-maps})]
    (-> row (update :segments decode) (update :capabilities decode) (update :degradations decode))))

(defn insert-outcome!
  "Accept content/outcome/source in one transaction and return the authoritative row.
  Duplicates return that row; conflicting logical identities roll back. Missing
  sessions return :missing-session. Publication must happen after this returns."
  [ds event]
  (jdbc/with-transaction [tx ds]
    (jdbc/execute-one! tx ["SET LOCAL statement_timeout = '30s'"])
    (if-let [session (session! tx event)]
      (let [recording-id (recording! tx event)]
        (jdbc/execute-one! tx
                           ["INSERT INTO session_transcripts (id,session_id,recording_id,tenant_id,user_id,full_text,lang,duration_s,segments,source,type,model,event_created_at_ns,result_id,plan_id,track_id,profile_id,status,is_primary,error_code,capabilities,degradations) VALUES (?,?,?,?,?,?,?,?,?::jsonb,'final-track','final',?,?,?,?,?,?,?,?,?,?::jsonb,?::jsonb) ON CONFLICT (result_id) DO NOTHING"
                            (UUID/randomUUID) (:id session) recording-id (:tenant_id session) (:user_id session)
                            (:full-text event) (:lang event) (/ (:sample-count event) (double (:sample-rate event)))
                            (encode (:segments event)) (:profile-id event) (:event-created-at-ns event)
                            (contract/uuid (:result-id event)) (contract/uuid (:plan-id event))
                            (:track-id event) (:profile-id event) (:status event) (:primary? session)
                            (not-empty (:error-code event)) (encode (:capabilities event)) (encode (:degradations event))])
        (let [accepted (accepted-result tx (:tenant-id event) (:session-id event) (:result-id event))]
          (when-not (and accepted (= recording-id (:recording_id accepted))
                         (= (:plan-id event) (str (:plan_id accepted)))
                         (= (:track-id event) (:track_id accepted))
                         (= (:profile-id event) (:profile_id accepted))
                         (= (:primary? session) (:is_primary accepted)))
            (contract/invalid! :result-conflict))
          (when (:is_primary accepted)
            (jdbc/execute-one! tx ["UPDATE sessions SET status='finished', ended_at=COALESCE(ended_at,now()) WHERE id=?"
                                   (:id session)]))
          accepted))
      :missing-session)))

(defn primary-projection
  "Build the unchanged legacy transcript from an accepted successful primary row."
  [row]
  (when (and (:is_primary row) (= "succeeded" (:status row)))
    (let [builder (doto (SessionTranscript/newBuilder)
                    (.setSessionId (str (:session_id row))) (.setTenantId (str (:tenant_id row)))
                    (.setRecordingUrl (:recording_url row)) (.setLang (or (:lang row) ""))
                    (.setDurationS (float (:duration_s row))) (.setFullText (:full_text row))
                    (.setCreatedAtNs (:event_created_at_ns row)))]
      (doseq [segment (:segments row) :when (contains? segment :start_s)]
        (let [out (doto (SessionTranscriptSegment/newBuilder)
                    (.setText (:text segment)) (.setStartS (double (:start_s segment)))
                    (.setEndS (double (:end_s segment))) (.setSpeaker (or (:speaker segment) "")))]
          (doseq [word (:words segment)]
            (.addWords out (.build (doto (WordAlignment/newBuilder)
                                     (.setText (:text word)) (.setStartS (double (:start_s word)))
                                     (.setEndS (double (:end_s word)))))))
          (.addSegments builder (.build out))))
      (.build builder))))

(defn insert-primary!
  "Recognize an already-persisted compatibility event; never insert a second row.
  A missing accepted result retries while its session exists; deleted sessions
  use the normal missing-session rejection without recreating transcript data."
  [ds ^SessionTranscript transcript metadata _segments]
  (if-let [accepted (accepted-result ds (.getTenantId transcript) (.getSessionId transcript) (:result-id metadata))]
    (if (= transcript (primary-projection accepted))
      :ok
      (contract/invalid! :primary-conflict))
    (if (jdbc/execute-one! ds ["SELECT id FROM sessions WHERE id=? AND tenant_id=?"
                               (contract/uuid (.getSessionId transcript))
                               (contract/uuid (.getTenantId transcript))])
      (throw (ex-info "Compatibility result is not yet accepted" {:type ::missing-accepted-result}))
      :missing-session)))
