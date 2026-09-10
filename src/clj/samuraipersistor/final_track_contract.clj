(ns samuraipersistor.final-track-contract
  "Validate bounded final-track identities and artifact references before SQL."
  (:require [jsonista.core :as json])
  (:import (java.nio ByteBuffer)
           (java.security MessageDigest)
           (java.util UUID HexFormat)
           (org.apache.kafka.clients.consumer ConsumerRecord)
           (samuraibff.proto FinalTrackResult AudioArtifact SessionTranscript)))

(def mapper (json/object-mapper {:decode-key-fn keyword :encode-key-fn name}))

(defn invalid!
  "Throw a sanitized permanent contract error; do not echo untrusted data."
  [reason]
  (throw (ex-info "Invalid final-track event" {:type ::invalid :reason reason})))

(defn uuid
  "Require the canonical string representation of a UUID."
  [value]
  (try
    (let [result (UUID/fromString (str value))]
      (when-not (= (str result) value) (invalid! :identity))
      result)
    (catch IllegalArgumentException _ (invalid! :identity))))

(defn digest
  "Compute the SHA-256 identity of serialized event bytes."
  [^bytes value]
  (.formatHex (HexFormat/of) (.digest (MessageDigest/getInstance "SHA-256") value)))

(defn uuid5
  "Derive an RFC 4122 UUIDv5, matching Python's stable run/result identities."
  [^UUID namespace-id ^String name]
  (let [buffer (doto (ByteBuffer/allocate 16)
                 (.putLong (.getMostSignificantBits namespace-id))
                 (.putLong (.getLeastSignificantBits namespace-id)))
        hash (doto (MessageDigest/getInstance "SHA-1") (.update (.array buffer)))
        bytes (.digest hash (.getBytes name "UTF-8"))]
    (aset-byte bytes 6 (unchecked-byte (bit-or 0x50 (bit-and 0x0f (aget bytes 6)))))
    (aset-byte bytes 8 (unchecked-byte (bit-or 0x80 (bit-and 0x3f (aget bytes 8)))))
    (let [result (ByteBuffer/wrap bytes)]
      (UUID. (.getLong result) (.getLong result)))))

(defn identities
  "Return deterministic [run-id result-id] for the selected final recording."
  [{:keys [tenant-id session-id plan-id source-id source-sha256 track-id profile-id]}]
  (let [run (uuid5 (UUID/fromString "6ba7b811-9dad-11d1-80b4-00c04fd430c8")
                   (json/write-value-as-string [tenant-id session-id plan-id source-id source-sha256
                                                "final" track-id profile-id]))]
    [(str run) (str (uuid5 run "recording:1"))]))

(defn source-map
  "Extract immutable source metadata from an AudioArtifact protobuf."
  [^AudioArtifact source]
  {:source-id (.getArtifactId source) :source-uri (.getStorageUri source)
   :source-sha256 (.getSha256 source) :source-size (.getSizeBytes source)
   :sample-count (.getSampleCount source) :sample-rate (.getSampleRate source)
   :version-id (.getVersionId source)})

(defn validate-identity!
  "Validate all identifiers, deterministic IDs, bounds, and the configured S3 scope."
  [{:keys [tenant-id session-id plan-id result-id run-id source-id source-uri source-sha256
           source-size sample-count sample-rate track-id profile-id] :as event}
   {:keys [bucket recording-prefix] :or {recording-prefix "recordings"}}]
  (doseq [value [tenant-id session-id plan-id result-id run-id source-id]] (uuid value))
  (when-not (and (seq bucket) (not (re-find #"[/\\:%]" bucket))
                 (every? #(and (string? %) (re-matches #"[a-z0-9][a-z0-9._-]{0,95}" %))
                         [track-id profile-id])
                 (string? source-sha256) (re-matches #"[a-f0-9]{64}" source-sha256)
                 (= 16000 sample-rate) (<= 1 sample-count 9600000)
                 (<= 44 source-size 19204096)
                 (= source-uri (str "s3://" bucket "/" recording-prefix "/"
                                    tenant-id "/" session-id "/" source-id ".wav"))
                 (= [run-id result-id] (identities event)))
    (invalid! :source-or-run))
  event)

(defn outcome
  "Parse and validate a final-only canonical envelope; return SQL-ready metadata."
  [^bytes value storage]
  (when (> (alength value) 65536) (invalid! :event-size))
  (let [^FinalTrackResult event (FinalTrackResult/parseFrom value)
        status (.getStatus event)
        data (merge (source-map (.getSource event))
                    {:tenant-id (.getTenantId event) :session-id (.getSessionId event)
                     :plan-id (.getPlanId event) :result-id (.getResultId event)
                     :run-id (.getRunId event) :attempt-id (.getAttemptId event)
                     :track-id (.getTrackId event) :profile-id (.getProfileId event)
                     :primary? (.getPrimary event) :status status
                     :result-uri (.getResultUri event) :result-sha256 (.getResultSha256 event)
                     :error-code (.getErrorCode event) :lang (.getLang event)
                     :event-created-at-ns (.getCreatedAtNs event) :event-sha256 (digest value)
                     :capabilities {:segment_timestamps (.getSegmentTimestamps event)
                                    :word_timestamps (.getWordTimestamps event)
                                    :speaker_labels (.getSpeakerLabels event)}
                     :degradations (vec (.getDegradationsList event))})]
    (validate-identity! data storage)
    (uuid (:attempt-id data))
    (when-not (and (= 1 (.getSchemaVersion event)) (= "final" (.getStage event))
                   (= "recording" (.getUnitId event)) (= 1 (.getRevision event))
                   (= "audio/wav" (.getMediaType (.getSource event)))
                   (contains? #{"succeeded" "failed"} status)
                   (pos? (:event-created-at-ns data)))
      (invalid! :envelope))
    (if (= "succeeded" status)
      (when-not (and (= (:result-uri data)
                        (str "s3://" (:bucket storage) "/final-tracks/"
                             (:tenant-id data) "/" (:session-id data) "/" (:source-id data) "/"
                             (:track-id data) "/" (:run-id data) "/" (:attempt-id data) ".transcript.json"))
                     (re-matches #"[a-f0-9]{64}" (:result-sha256 data))
                     (empty? (:error-code data)))
        (invalid! :result-artifact))
      (when-not (and (empty? (:result-uri data)) (empty? (:result-sha256 data))
                     (re-matches #"[a-z_]{1,96}" (:error-code data)))
        (invalid! :failure)))
    (let [provenance (try (json/read-value (.getProvenanceJson event) mapper)
                          (catch Exception _ (invalid! :provenance)))]
      (when-not (map? provenance) (invalid! :provenance))
      (assoc data :provenance provenance))))

(def projection-header-names
  ["x-result-id" "x-asr-plan-id" "x-run-id" "x-final-track-id" "x-final-profile-id"
   "x-source-artifact-id" "x-source-sha256" "x-source-size-bytes"
   "x-source-sample-count" "x-source-sample-rate" "x-source-version-id"])

(defn projection-headers
  "Read all stable primary-projection headers; partial identity is an error."
  [^ConsumerRecord record]
  (let [selected (filter #(contains? (set projection-header-names) (.key %)) (.headers record))]
    (when (seq selected)
      (when-not (and (= (count selected) (count projection-header-names))
                     (= (set projection-header-names) (set (map #(.key %) selected))))
        (invalid! :projection-headers))
      (let [headers (into {} (map (fn [header]
                                    (when (or (nil? (.value header)) (> (alength ^bytes (.value header)) 1024))
                                      (invalid! :projection-headers))
                                    [(.key header) (String. ^bytes (.value header) "UTF-8")]) selected))]
        (try
          {:result-id (headers "x-result-id") :plan-id (headers "x-asr-plan-id")
           :run-id (headers "x-run-id") :track-id (headers "x-final-track-id")
           :profile-id (headers "x-final-profile-id") :source-id (headers "x-source-artifact-id")
           :source-sha256 (headers "x-source-sha256") :version-id (headers "x-source-version-id")
           :source-size (Long/parseLong (headers "x-source-size-bytes"))
           :sample-count (Long/parseLong (headers "x-source-sample-count"))
           :sample-rate (Long/parseLong (headers "x-source-sample-rate"))
           :event-sha256 (digest (.value record))}
          (catch NumberFormatException _ (invalid! :projection-headers)))))))

(defn primary
  "Validate source identity on an identified primary compatibility event."
  [^SessionTranscript event headers storage]
  (let [data (merge headers {:tenant-id (.getTenantId event) :session-id (.getSessionId event)
                             :source-uri (.getRecordingUrl event) :lang (.getLang event) :primary? true})]
    (validate-identity! data storage)
    (when (> (Math/abs (- (.getDurationS event) (/ (:sample-count data) 16000.0))) 0.001)
      (invalid! :duration))
    data))
