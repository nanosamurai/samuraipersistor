(ns samuraipersistor.system
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [integrant.core :as ig]))

(defn- env-value
  "Fetch an environment variable and coerce it when needed.

  Arguments:
  - env-key: environment variable name
  - parser: function to parse the string value (optional)

  Returns nil when the env var is missing or blank."
  ([env-key] (env-value env-key identity))
  ([env-key parser]
   (let [raw (System/getenv env-key)]
     (when (and raw (not (string/blank? raw)))
       (parser raw)))))

(defn- parse-int [value]
  (Integer/parseInt value))

(defn- parse-long [value]
  (Long/parseLong value))

(defn- apply-env-overrides
  "Apply environment variable overrides onto the system config map.

  Uses the SP_ prefixed env vars so the service can be configured in k8s.
  Returns the updated config map." 
  [config]
  (let [updates {:http {:host (env-value "SP_HTTP_HOST")
                        :port (env-value "SP_HTTP_PORT" parse-int)}
                 :db {:jdbc-url (env-value "SP_DB_JDBC_URL")
                      :username (env-value "SP_DB_USERNAME")
                      :password (env-value "SP_DB_PASSWORD")
                      :maximum-pool-size (env-value "SP_DB_MAX_POOL_SIZE" parse-int)
                      :minimum-idle (env-value "SP_DB_MIN_IDLE" parse-int)
                      :connection-timeout-ms (env-value "SP_DB_CONN_TIMEOUT_MS" parse-long)}
                 :kafka {:bootstrap-servers (env-value "SP_KAFKA_BOOTSTRAP_SERVERS")
                         :client-id (env-value "SP_KAFKA_CLIENT_ID")
                         :refined-consumer-group-id (env-value "SP_KAFKA_REFINED_GROUP_ID")
                         :final-consumer-group-id (env-value "SP_KAFKA_FINAL_GROUP_ID")
                         :topics {:refined (env-value "SP_KAFKA_TOPIC_REFINED")
                                  :final (env-value "SP_KAFKA_TOPIC_FINAL")
                                  :dlq (env-value "SP_KAFKA_TOPIC_DLQ")}
                         :max-poll-records (env-value "SP_KAFKA_MAX_POLL_RECORDS" parse-int)
                         :max-poll-interval-ms (env-value "SP_KAFKA_MAX_POLL_INTERVAL_MS" parse-int)
                         :session-timeout-ms (env-value "SP_KAFKA_SESSION_TIMEOUT_MS" parse-int)
                         :refined-buffer-size (env-value "SP_KAFKA_REFINED_BUFFER_SIZE" parse-int)
                         :final-buffer-size (env-value "SP_KAFKA_FINAL_BUFFER_SIZE" parse-int)}}
        merge-kv (fn [m k v]
                   (if (nil? v)
                     m
                     (assoc m k v)))
        merge-map (fn [base override]
                    (reduce-kv merge-kv base override))]
    (-> config
        (update :samuraipersistor/config
                (fn [cfg]
                  (-> cfg
                      (update :http merge-map (:http updates))
                      (update :db merge-map (:db updates))
                      (update :kafka
                              (fn [kafka-cfg]
                                (-> kafka-cfg
                                    (merge-map (dissoc (:kafka updates) :topics))
                                    (update :topics merge-map (get-in updates [:kafka :topics])))))))))))

(defn read-system-config
  "Read Integrant config from resources/system.edn.

  We use Integrant's reader so `#ig/ref` tags are supported.
  Environment variables prefixed with SP_ can override values." 
  ([] (read-system-config "system.edn"))
  ([resource-name]
   (-> (io/resource resource-name)
       slurp
       ig/read-string
       apply-env-overrides)))

(defn start!
  "Initialize and start the full Integrant system." 
  ([] (start! (read-system-config)))
  ([config]
   (ig/load-namespaces config)
   (ig/init config)))

(defn stop!
  "Halt an Integrant system. Idempotent." 
  [system]
  (when system
    (ig/halt! system)))
