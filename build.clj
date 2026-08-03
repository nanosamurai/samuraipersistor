(ns build
  (:require [clojure.tools.build.api :as b]))

(def class-dir "target/classes")
(def uber-file "target/samuraipersistor.jar")
(def basis (b/create-basis {:project "deps.edn"}))

(defn clean [_]
  (b/delete {:path "target"})
  nil)

(defn compile-java [_]
  (b/javac {:src-dirs  ["src/java"]
            :class-dir class-dir
            :basis     basis})
  nil)

(defn uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs   ["resources"]
               :target-dir class-dir})
  (compile-java nil)
  (b/compile-clj {:basis     basis
                  :src-dirs  ["src/clj"]
                  :class-dir class-dir})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis     basis
           :main      'samuraipersistor.core
           :exclude   ["(?i)^META-INF/license(/.*)?$"]})
  nil)
