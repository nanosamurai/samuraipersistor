(ns samuraipersistor.final-track-contract
  "Validate bounded final-track identities and inline transcripts before SQL."
  (:require [jsonista.core :as json])
  (:import (java.nio ByteBuffer)
           (java.security MessageDigest)
           (java.util UUID)
           (org.apache.kafka.clients.consumer ConsumerRecord)
           (samuraibff.proto FinalTrackResult AudioArtifact)))

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
  [{:keys [tenant-id session-id plan-id result-id source-id source-uri source-sha256
           source-size sample-count sample-rate track-id profile-id] :as event}
   {:keys [bucket recording-prefix] :or {recording-prefix "recordings"}}]
  (doseq [value [tenant-id session-id plan-id result-id source-id]] (uuid value))
  (when-not (and (seq bucket) (not (re-find #"[/\\:%]" bucket))
                 (every? #(and (string? %) (re-matches #"[a-z0-9][a-z0-9._-]{0,95}" %))
                         [track-id profile-id])
                 (string? source-sha256) (re-matches #"[a-f0-9]{64}" source-sha256)
                 (= 16000 sample-rate) (<= 1 sample-count 9600000)
                 (<= 44 source-size 19204096)
                 (= source-uri (str "s3://" bucket "/" recording-prefix "/"
                                    tenant-id "/" session-id "/" source-id ".wav"))
                 (= result-id (second (identities event))))
    (invalid! :source-or-run))
  event)

(def max-result-bytes 900000)
(def max-record-bytes 1000000)

(defn legacy-contract!
  "Stop old reference-contract input for explicit conversion without committing it."
  []
  (throw (ex-info "Convert retained reference-contract input before enabling inline results"
                  {:type ::legacy-contract})))

(defn segment-map
  "Decode a segment, omitting timing when the event declares it unavailable."
  [segment timed?]
  (cond-> {:text (.getText segment)}
    timed? (assoc :start_s (.getStartS segment) :end_s (.getEndS segment))
    (seq (.getSpeaker segment)) (assoc :speaker (.getSpeaker segment))
    (pos? (.getWordsCount segment))
    (assoc :words (mapv (fn [word] {:text (.getText word) :start_s (.getStartS word)
                                    :end_s (.getEndS word)}) (.getWordsList segment)))))

(defn- valid-time?
  "Check finite recording-relative bounds, allowing an empty segment."
  [start end duration]
  (and (number? start) (number? end) (Double/isFinite (double start))
       (Double/isFinite (double end)) (<= 0 start end (+ duration 0.01))))

(defn outcome
  "Validate schema 2 inline content and identity; old schema requires explicit conversion."
  [^bytes value storage]
  (when (> (alength value) max-result-bytes) (invalid! :event-size))
  (let [^FinalTrackResult event (FinalTrackResult/parseFrom value)
        _ (when (= 1 (.getSchemaVersion event)) (legacy-contract!))
        data (merge (source-map (.getSource event))
                    {:tenant-id (.getTenantId event) :session-id (.getSessionId event)
                     :plan-id (.getPlanId event) :result-id (.getResultId event)
                     :track-id (.getTrackId event) :profile-id (.getProfileId event)
                     :status (.getStatus event) :error-code (.getErrorCode event)
                     :lang (.getLang event) :event-created-at-ns (.getCreatedAtNs event)
                     :full-text (.getFullText event)
                     :segments (mapv #(segment-map % (.getSegmentTimestamps event)) (.getSegmentsList event))
                     :capabilities {:segment_timestamps (.getSegmentTimestamps event)
                                    :word_timestamps (.getWordTimestamps event)
                                    :speaker_labels (.getSpeakerLabels event)}
                     :degradations (vec (.getDegradationsList event))})
        duration (/ (:sample-count data) 16000.0)]
    (validate-identity! data storage)
    (when-not (and (= 2 (.getSchemaVersion event))
                   (empty? (.asMap (.getUnknownFields event)))
                   (= "audio/wav" (.getMediaType (.getSource event)))
                   (contains? #{"succeeded" "failed"} (:status data))
                   (pos? (:event-created-at-ns data)) (<= (count (:lang data)) 32)
                   (<= (count (:version-id data)) 1024)
                   (<= (count (:degradations data)) 32)
                   (every? #(re-matches #"[a-z0-9_]{1,96}" %) (:degradations data)))
      (invalid! :envelope))
    (if (= "failed" (:status data))
      (when-not (and (empty? (:full-text data)) (empty? (:segments data))
                     (every? false? (vals (:capabilities data)))
                     (re-matches #"[a-z_]{1,96}" (:error-code data)))
        (invalid! :failure))
      (when-not (empty? (:error-code data)) (invalid! :success)))
    (when-not (and (= (.getWordTimestamps event) (boolean (some :words (:segments data))))
                   (= (.getSpeakerLabels event) (boolean (some :speaker (:segments data))))
                   (or (seq (:segments data)) (not (.getSegmentTimestamps event))))
      (invalid! :availability))
    (doseq [segment (:segments data)]
      (when (and (.getSegmentTimestamps event)
                 (not (valid-time? (:start_s segment) (:end_s segment) duration)))
        (invalid! :segment-time))
      (doseq [word (:words segment)]
        (when-not (and (valid-time? (:start_s word) (:end_s word) duration)
                       (< (:start_s word) (:end_s word)))
          (invalid! :word-time))))
    data))

(defn projection-headers
  "Identify Persistor's compatibility events; old headers require explicit conversion."
  [^ConsumerRecord record]
  (let [headers (.headers record)
        ids (vec (.headers headers "x-result-id"))
        versions (vec (.headers headers "x-final-track-contract"))]
    (when (or (seq ids) (seq versions))
      (when-not (seq versions) (legacy-contract!))
      (when-not (and (= 1 (count ids)) (= 1 (count versions))
                     (= "2" (String. ^bytes (.value (first versions)) "UTF-8"))
                     (= 36 (alength ^bytes (.value (first ids)))))
        (invalid! :projection-headers))
      {:result-id (str (uuid (String. ^bytes (.value (first ids)) "UTF-8")))})))
