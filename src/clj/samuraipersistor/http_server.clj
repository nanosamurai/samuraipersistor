(ns samuraipersistor.http-server
  (:require [clojure.string :as str]
            [integrant.core :as ig]
            [jsonista.core :as json]
            [org.corfield.logging4j2 :as log]
            [org.httpkit.server :as http]
            [reitit.ring :as ring]
            [ring.util.response :as resp]
            [next.jdbc :as jdbc])
  (:import (java.time Instant)))

(defn- edn-request?
  [req]
  (some-> (get-in req [:headers "accept"])
          str/lower-case
          (str/includes? "application/edn")))

(defn- ok-response
  [req body]
  (let [edn? (edn-request? req)]
    (-> (resp/response (if edn?
                         (pr-str body)
                         (json/write-value-as-string body)))
        (resp/content-type (if edn?
                             "application/edn; charset=utf-8"
                             "application/json; charset=utf-8")))))

(defn- health-handler [req]
  (ok-response req {:status :ok
                    :service :samuraipersistor
                    :time (str (Instant/now))}))

(defn- ready-handler
  "Readiness check.

  Currently checks DB connectivity.
  (Kafka connectivity is implied by consumer threads; we can extend with explicit checks later.)" 
  [{:keys [ds]}]
  (fn [req]
    (try
      (jdbc/execute-one! ds ["SELECT 1 AS ok"])
      (ok-response req {:status :ready})
      (catch Throwable t
        (log/warn t "Readiness check failed")
        (-> (ok-response req {:status :not-ready
                              :error "db"})
            (resp/status 503))))))

(defn router
  "Build the Ring handler for service health probes.

  Expects a map containing the initialized DB component under `:db` and returns
  a Ring handler. Readiness failures are converted to HTTP 503 responses."
  [{:keys [db]}]
  (ring/ring-handler
    (ring/router
      [["/health" {:get health-handler}]
       ["/ready" {:get (ready-handler db)}]])
    (ring/create-default-handler)))

(defmethod ig/init-key :samuraipersistor/http-server
  [_ {:keys [config db]}]
  (let [{:keys [host port]} (get-in config [:http])
        handler (router {:db db})
        stop-fn (http/run-server handler {:ip host :port port})]
    (log/info "HTTP server started" {:host host :port port})
    {:stop-fn stop-fn}))

(defmethod ig/halt-key! :samuraipersistor/http-server
  [_ {:keys [stop-fn]}]
  (when stop-fn
    (log/info "Stopping HTTP server")
    (stop-fn :timeout 1000)))
