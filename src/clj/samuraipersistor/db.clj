(ns samuraipersistor.db
  (:require [integrant.core :as ig]
            [next.jdbc :as jdbc]
            [org.corfield.logging4j2 :as log])
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource)
           (javax.sql DataSource)))

(defn make-datasource
  "Create a HikariCP DataSource from system config.

  Expected config keys under [:db]:
  - :jdbc-url
  - :username
  - :password
  - :maximum-pool-size
  - :minimum-idle (optional)
  - :connection-timeout-ms (optional)" 
  [{:keys [jdbc-url username password maximum-pool-size minimum-idle connection-timeout-ms]}]
  (doto (HikariConfig.)
    (.setJdbcUrl jdbc-url)
    (.setUsername username)
    (.setPassword password)
    (.setMaximumPoolSize (int (or maximum-pool-size 10)))
    (.setMinimumIdle (int (or minimum-idle 2)))
    (.setConnectionTimeout (long (or connection-timeout-ms 5000)))
    ;; perf + safety defaults
    (.setAutoCommit true)
    (.setPoolName "samuraipersistor-hikari")))

(defmethod ig/init-key :samuraipersistor/db
  [_ {:keys [config]}]
  (let [db-cfg (get config :db)
        ds (HikariDataSource. (make-datasource db-cfg))]
    ;; Fail fast on boot if DB misconfigured.
    (with-open [c (.getConnection ds)]
      (log/info "DB connection OK" {:db-url (:jdbc-url db-cfg)}))
    {:ds ds}))

(defmethod ig/halt-key! :samuraipersistor/db
  [_ {:keys [^DataSource ds]}]
  (when (instance? HikariDataSource ds)
    (log/info "Closing DB pool")
    (.close ^HikariDataSource ds)))

(defn execute! [db sql params]
  (jdbc/execute! (:ds db) (into [sql] params)))

(defn execute-one! [db sql params]
  (jdbc/execute-one! (:ds db) (into [sql] params)))
