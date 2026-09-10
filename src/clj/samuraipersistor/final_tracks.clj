(ns samuraipersistor.final-tracks
  "Tenant-scoped, idempotent canonical outcomes and primary transcript projections."
  (:require [jsonista.core :as json]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [samuraipersistor.final-track-contract :as contract])
  (:import (java.util UUID)
           (org.postgresql.util PGobject)
           (samuraibff.proto SessionTranscript RefinedEvent)))

(defn- encode
  "Encode a JSONB parameter using the shared contract mapper."
  [value]
  (json/write-value-as-string value contract/mapper))

(defn- session!
  "Resolve an owned session and verify its frozen plan selection; nil means missing."
  [tx {:keys [tenant-id session-id plan-id track-id profile-id primary? stage window-samples]}]
  (when-let [row (jdbc/execute-one! tx
                                    ["SELECT id, tenant_id, user_id, stream_controls FROM sessions WHERE id=? AND tenant_id=? FOR UPDATE"
                                     (contract/uuid session-id) (contract/uuid tenant-id)]
                                    {:builder-fn rs/as-unqualified-lower-maps})]
    (let [raw (:stream_controls row)
          controls (if (instance? PGobject raw)
                     (json/read-value (.getValue ^PGobject raw) contract/mapper) raw)
          plan (:asr_plan controls)
          selection (some #(when (= track-id (:track_id %)) %)
                          (get plan (if (= stage "refined") :refinement_tracks :final_tracks)))]
      (when-not (and (= plan-id (:plan_id plan)) (= tenant-id (:tenant_id plan))
                     (= session-id (:session_id plan)) (= profile-id (:profile_id selection))
                     (= primary? (:primary selection))
                     (or (not= stage "refined") (= window-samples (:refinement_window_samples plan))))
        (contract/invalid! :session-plan))
      row)))

(defn- recording!
  "Insert or reuse the one recording row; reject a reused ID with different bytes."
  [tx {:keys [session-id source-id source-uri source-sha256 source-size sample-count sample-rate version-id lang]}]
  (let [row (jdbc/execute-one! tx
                               ["INSERT INTO recordings (id,session_id,recording_url,duration_s,sample_rate,lang,source_artifact_id,source_sha256,source_size_bytes,source_sample_count,source_version_id) VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (session_id,source_artifact_id) DO UPDATE SET id=recordings.id WHERE recordings.recording_url=EXCLUDED.recording_url AND recordings.source_sha256=EXCLUDED.source_sha256 AND recordings.source_size_bytes=EXCLUDED.source_size_bytes AND recordings.source_sample_count=EXCLUDED.source_sample_count AND recordings.sample_rate=EXCLUDED.sample_rate AND recordings.source_version_id=EXCLUDED.source_version_id RETURNING id"
                                (UUID/randomUUID) (contract/uuid session-id) source-uri (/ sample-count (double sample-rate))
                                sample-rate lang (contract/uuid source-id) source-sha256 source-size sample-count version-id]
                               {:builder-fn rs/as-unqualified-lower-maps})]
    (or (:id row) (contract/invalid! :source-conflict))))

(defn insert-outcome!
  "Persist validated canonical metadata; duplicates must have the same event digest.
  Returns :ok or :missing-session. Errors roll back without changing earlier rows."
  [ds event]
  (jdbc/with-transaction [tx ds]
    (jdbc/execute-one! tx ["SET LOCAL statement_timeout = '30s'"])
    (if-not (session! tx event)
      :missing-session
      (let [refined? (= "refined" (:stage event))
            recording-id (when-not refined? (recording! tx event))
            source (select-keys event [:source-id :source-uri :source-sha256 :source-size :sample-count :sample-rate :version-id])
            previous (when refined?
                       (jdbc/execute-one! tx
                                          ["SELECT source FROM transcript_track_results WHERE tenant_id=? AND session_id=? AND source->>'source-id'=? LIMIT 1"
                                           (contract/uuid (:tenant-id event)) (contract/uuid (:session-id event)) (:source-id event)]
                                          {:builder-fn rs/as-unqualified-lower-maps}))
            _ (when (and previous
                         (not= source (json/read-value (.getValue ^PGobject (:source previous)) contract/mapper)))
                (contract/invalid! :source-conflict))
            row (jdbc/execute-one! tx
                                   ["INSERT INTO transcript_track_results (result_id,tenant_id,session_id,recording_id,plan_id,track_id,profile_id,run_id,attempt_id,stage,unit_id,revision,status,is_primary,result_uri,result_sha256,error_code,capabilities,degradations,provenance,event_sha256,event_created_at_ns,source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?::jsonb,?::jsonb,?::jsonb,?,?,?::jsonb) ON CONFLICT (result_id) DO UPDATE SET result_id=transcript_track_results.result_id WHERE transcript_track_results.event_sha256=EXCLUDED.event_sha256 AND transcript_track_results.tenant_id=EXCLUDED.tenant_id RETURNING result_id"
                                    (contract/uuid (:result-id event)) (contract/uuid (:tenant-id event))
                                    (contract/uuid (:session-id event)) recording-id (contract/uuid (:plan-id event))
                                    (:track-id event) (:profile-id event) (contract/uuid (:run-id event))
                                    (contract/uuid (:attempt-id event)) (or (:stage event) "final")
                                    (or (:unit-id event) "recording") (or (:revision event) 1) (:status event) (:primary? event)
                                    (not-empty (:result-uri event)) (not-empty (:result-sha256 event))
                                    (not-empty (:error-code event)) (encode (:capabilities event))
                                    (encode (:degradations event)) (encode (:provenance event))
                                    (:event-sha256 event) (:event-created-at-ns event) (encode source)]
                                   {:builder-fn rs/as-unqualified-lower-maps})]
        (when-not (:result_id row) (contract/invalid! :result-conflict))
        :ok))))

(defn insert-refined-primary!
  "Persist the accepted primary window once without finishing the recording session."
  [ds ^RefinedEvent transcript event segments]
  (when-not (and (= "refined" (:stage event)) (:primary? event) (= "succeeded" (:status event))
                 (= (:tenant-id event) (.getTenantId transcript))
                 (= (:session-id event) (.getSessionId transcript))
                 (= (:profile-id event) (.getRefinementModel transcript))
                 (= (:event-created-at-ns event) (.getCreatedAtNs transcript))
                 (= (/ (:start-sample event) 16000.0) (.getStartS transcript))
                 (= (/ (:end-sample event) 16000.0) (.getEndS transcript))
                 (= (/ (:window-samples event) 16000.0) (.getWindowSec transcript)))
    (contract/invalid! :refined-projection))
  (jdbc/with-transaction [tx ds]
    (if (= :missing-session (insert-outcome! tx event))
      :missing-session
      (let [session (session! tx event)
            row (jdbc/execute-one! tx
                                   ["INSERT INTO session_transcripts (id,session_id,tenant_id,user_id,full_text,lang,segments,source,type,model,window_length,segment_start_s,segment_end_s,event_created_at_ns,result_id,result_event_sha256) VALUES (?,?,?,?,?,?,?::jsonb,'refined-track','refined',?,?,?,?,?,?,?) ON CONFLICT (result_id) DO UPDATE SET result_id=session_transcripts.result_id WHERE session_transcripts.result_event_sha256=EXCLUDED.result_event_sha256 AND session_transcripts.tenant_id=EXCLUDED.tenant_id RETURNING id"
                                    (UUID/randomUUID) (:id session) (:tenant_id session) (:user_id session)
                                    (.getText transcript) (.getLang transcript) (encode segments)
                                    (:profile-id event) (long (.getWindowSec transcript))
                                    (.getStartS transcript) (.getEndS transcript) (.getCreatedAtNs transcript)
                                    (contract/uuid (:result-id event)) (contract/digest (.toByteArray transcript))]
                                   {:builder-fn rs/as-unqualified-lower-maps})]
        (when-not (:id row) (contract/invalid! :primary-conflict))
        :ok))))

(defn insert-primary!
  "Persist an identified primary once, reusing the source row in either arrival order."
  [ds ^SessionTranscript transcript metadata segments]
  (let [event (contract/primary transcript metadata (:storage metadata))]
    (jdbc/with-transaction [tx ds]
      (jdbc/execute-one! tx ["SET LOCAL statement_timeout = '30s'"])
      (if-let [session (session! tx event)]
        (let [recording-id (recording! tx event)
              row (jdbc/execute-one! tx
                                     ["INSERT INTO session_transcripts (id,session_id,recording_id,tenant_id,user_id,full_text,lang,duration_s,segments,source,type,model,event_created_at_ns,result_id,result_event_sha256) VALUES (?,?,?,?,?,?,?,?,?::jsonb,?,'final',?,?,?,?) ON CONFLICT (result_id) DO UPDATE SET result_id=session_transcripts.result_id WHERE session_transcripts.result_event_sha256=EXCLUDED.result_event_sha256 AND session_transcripts.tenant_id=EXCLUDED.tenant_id RETURNING id"
                                      (UUID/randomUUID) (:id session) recording-id (:tenant_id session) (:user_id session)
                                      (.getFullText transcript) (.getLang transcript) (.getDurationS transcript)
                                      (encode segments) "final-track" (:profile-id event) (.getCreatedAtNs transcript)
                                      (contract/uuid (:result-id event)) (:event-sha256 event)]
                                     {:builder-fn rs/as-unqualified-lower-maps})]
          (when-not (:id row) (contract/invalid! :primary-conflict))
          (jdbc/execute-one! tx ["UPDATE sessions SET status='finished', ended_at=COALESCE(ended_at,now()) WHERE id=?"
                                 (:id session)])
          :ok)
        :missing-session))))
