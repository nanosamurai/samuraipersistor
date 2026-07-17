(ns samuraipersistor.http-server-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [samuraipersistor.http-server :as http-server]))

(defn- request
  [handler uri]
  (handler {:request-method :get
            :uri uri}))

(deftest health-route-test
  (testing "health reports that the HTTP process is live"
    (let [response (request (http-server/router {:db {:ds ::datasource}})
                            "/health")]
      (is (= 200 (:status response)))
      (is (= :ok (:status (edn/read-string (:body response))))))))

(deftest ready-route-test
  (testing "readiness checks the configured datasource"
    (let [datasource ::datasource
          checked-datasource (atom nil)
          handler (http-server/router {:db {:ds datasource}})]
      (with-redefs [jdbc/execute-one! (fn [ds sql]
                                       (reset! checked-datasource ds)
                                       (is (= ["SELECT 1 AS ok"] sql))
                                       {:ok 1})]
        (let [response (request handler "/ready")]
          (is (= 200 (:status response)))
          (is (= {:status :ready}
                 (edn/read-string (:body response))))
          (is (identical? datasource @checked-datasource))))))

  (testing "readiness returns 503 when the database check fails"
    (let [handler (http-server/router {:db {:ds ::datasource}})]
      (with-redefs [jdbc/execute-one! (fn [_ds _sql]
                                       (throw (ex-info "Database unavailable" {})))]
        (let [response (request handler "/ready")]
          (is (= 503 (:status response)))
          (is (= {:status :not-ready
                  :error "db"}
                 (edn/read-string (:body response)))))))))
