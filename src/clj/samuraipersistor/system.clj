(ns samuraipersistor.system
  (:require [clojure.java.io :as io]
            [integrant.core :as ig]))

(defn read-system-config
  "Read Integrant config from resources/system.edn.

  We use Integrant's reader so `#ig/ref` tags are supported." 
  ([] (read-system-config "system.edn"))
  ([resource-name]
   (-> (io/resource resource-name)
       slurp
       ig/read-string)))

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
