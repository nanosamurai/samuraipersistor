(ns samuraipersistor.http-server
  (:require [integrant.core :as ig]
            [org.corfield.logging4j2 :as log]
            [org.httpkit.server :as http]
            [reitit.ring :as ring]
            [ring.util.response :as resp]
            [next.jdbc :as jdbc])
  (:import (java.time Instant)))

(defn- ok-json [m]
  (-> (resp/response (pr-str m))
      (resp/content-type "application/edn; charset=utf-8")))

(defn- health-handler [_req]
  (ok-json {:status :ok
            :service :samuraipersistor
            :time (str (Instant/now))}))

(defn- ready-handler
  "Readiness check.

  Currently checks DB connectivity.
  (Kafka connectivity is implied by consumer threads; we can extend with explicit checks later.)" 
  [{:keys [ds]}]
  (fn [_req]
    (try
      (jdbc/execute-one! ds ["SELECT 1 AS ok"])
      (ok-json {:status :ready})
      (catch Throwable t
        (log/warn t "Readiness check failed")
        (-> (ok-json {:status :not-ready
                      :error "db"})
            (resp/status 503))))))

(defn router
  [{:keys [db]}]
  (ring/ring-handler
    (ring/router
      [["/health" {:get health-handler}]
       ["/ready" {:get (ready-handler (:ds db))}]])
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
