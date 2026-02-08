(ns samuraipersistor.core
  (:gen-class)
  (:require [org.corfield.logging4j2 :as log]
            [samuraipersistor.system :as system]))

(defonce ^:private running-system (atom nil))

(defn -main
  "Entrypoint for `clojure -M:run`.

  Starts the Integrant system from resources/system.edn and installs a JVM shutdown hook." 
  [& _args]
  (log/info "Starting samuraipersistor")
  (reset! running-system (system/start!))
  (.addShutdownHook
    (Runtime/getRuntime)
    (Thread.
      (fn []
        (log/info "Shutdown hook: halting system")
        (try
          (system/stop! @running-system)
          (catch Throwable t
            (log/error t "Failed to halt system"))))))
  ;; keep main thread alive
  @(promise))
