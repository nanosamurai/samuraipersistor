(ns samuraipersistor.otel.traceparent
  "W3C traceparent extraction + explicit OTEL context binding.

  Why this exists
  --------------
  samuraipersistor polls Kafka on one thread and processes records on worker
  threads via a LinkedBlockingQueue.

  Even with the OpenTelemetry Java agent enabled, context propagation is not
  guaranteed across arbitrary thread handoffs.

  To ensure end-to-end traces (trace_id == session_id_without_dashes) include
  persistor work, we explicitly:
  - extract `traceparent` from ConsumerRecord headers
  - create a remote parent SpanContext
  - make it current for the duration of persistence work
  
  This is safe even when traceparent is missing/invalid (no-op).
  "
  (:require [clojure.string :as str])
  (:import (io.opentelemetry.api.trace Span SpanContext TraceFlags TraceState)
           (io.opentelemetry.context Context Scope)
           (org.apache.kafka.clients.consumer ConsumerRecord)
           (org.apache.kafka.common.header Header)
           (java.nio.charset StandardCharsets)))

;; NOTE: This file must be saved as UTF-8 without BOM. A BOM can cause the
;; Clojure reader to fail on Windows.

(defn- header-bytes
  "Return Kafka header bytes for `key` (case-insensitive), or nil."
  [^ConsumerRecord rec ^String key]
  (when (and rec key)
    (let [hdrs (.headers rec)
          key-l (str/lower-case key)]
      (when hdrs
        (some (fn [^Header h]
                (when (= key-l (str/lower-case (.key h)))
                  (.value h)))
              (iterator-seq (.iterator hdrs)))))))

(defn traceparent-from-record
  "Extract W3C traceparent header value from a ConsumerRecord, or nil."
  [^ConsumerRecord rec]
  (when-let [b (header-bytes rec "traceparent")]
    (try
      (String. ^bytes b StandardCharsets/UTF_8)
      (catch Throwable _
        nil))))

(def ^:private traceparent-re
  ;; version-traceid-spanid-flags
  ;; Example: 00-<32hex>-<16hex>-01
  (re-pattern "(?i)^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2}).*$"))

(defn context-from-traceparent
  "Parse a W3C traceparent header and return an OTEL Context with remote parent.

  Returns nil if invalid."
  [^String tp]
  (when-let [m (and tp (re-matches traceparent-re (str/trim tp)))]
    (let [trace-id (str/lower-case (nth m 2))
          span-id  (str/lower-case (nth m 3))
          flags    (str/lower-case (nth m 4))
          sampled? (= flags "01")
          tf (if sampled? (TraceFlags/getSampled) (TraceFlags/getDefault))
          sc (SpanContext/createFromRemoteParent trace-id span-id tf (TraceState/getDefault))
          span (Span/wrap sc)]
      (.with (Context/root) span))))

(defmacro with-record-trace
  "Bind OTEL context extracted from record.traceparent for duration of body.

  If traceparent missing/invalid, executes body without modifying context." 
  [rec & body]
  `(if-let [ctx# (some-> ~rec traceparent-from-record context-from-traceparent)]
     (let [^Scope scope# (.makeCurrent ^Context ctx#)]
       (try
         ~@body
         (finally
           (.close scope#))))
     (do
       ~@body)))

