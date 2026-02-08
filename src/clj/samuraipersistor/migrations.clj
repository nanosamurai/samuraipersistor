(ns samuraipersistor.migrations
  (:require [migratus.core :as migratus]
            [org.corfield.logging4j2 :as log]))

(defn migratus-config
  "Build Migratus config.

  Inputs:
  - :jdbc-url (string)
  - :username (string)
  - :password (string)

  Migration files live under `resources/migrations`." 
  [{:keys [jdbc-url username password]}]
  {:store :database
   :migration-dir "migrations"
   :db {:connection-uri jdbc-url
        :user username
        :password password}})

(defn migrate!
  "Run all pending migrations." 
  [{:keys [jdbc-url username password] :as args}]
  (log/info "Running migrations" (dissoc args :password))
  (migratus/migrate (migratus-config {:jdbc-url jdbc-url
                                     :username username
                                     :password password})))

(defn rollback!
  "Rollback the last applied migration." 
  [{:keys [jdbc-url username password] :as args}]
  (log/warn "Rolling back last migration" (dissoc args :password))
  (migratus/rollback (migratus-config {:jdbc-url jdbc-url
                                      :username username
                                      :password password})))
