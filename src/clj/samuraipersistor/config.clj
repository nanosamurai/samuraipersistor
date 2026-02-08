(ns samuraipersistor.config
  (:require [integrant.core :as ig]))

(defmethod ig/init-key :samuraipersistor/config
  [_ config]
  config)
