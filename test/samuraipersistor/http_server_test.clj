(ns samuraipersistor.http-server-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing]]
            [jsonista.core :as json]
            [next.jdbc :as jdbc]
            [samuraipersistor.http-server :as http-server]))

(defn- request
  ([handler uri]
   (request handler uri {}))
  ([handler uri headers]
   (handler {:request-method :get
             :uri uri
             :headers headers})))

(defn- json-body
  [response]
  (json/read-value (:body response) (json/object-mapper {:decode-key-fn keyword})))

(deftest health-route-test
  (testing "health defaults to JSON and reports that the HTTP process is live"
    (let [response (request (http-server/router {:db {:ds ::datasource}})
                            "/health")]
      (is (= 200 (:status response)))
      (is (= "application/json; charset=utf-8"
             (get-in response [:headers "Content-Type"])))
      (is (= "ok" (:status (json-body response))))))

  (testing "health preserves EDN when the client explicitly requests it"
    (let [response (request (http-server/router {:db {:ds ::datasource}})
                            "/health"
                            {"accept" "application/edn"})]
      (is (= "application/edn; charset=utf-8"
             (get-in response [:headers "Content-Type"])))
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
        (let [response (request handler "/ready" {"accept" "application/json"})]
          (is (= 200 (:status response)))
          (is (= "application/json; charset=utf-8"
                 (get-in response [:headers "Content-Type"])))
          (is (= {:status "ready"}
                 (json-body response)))
          (is (identical? datasource @checked-datasource))))))

  (testing "readiness returns 503 when the database check fails"
    (let [handler (http-server/router {:db {:ds ::datasource}})]
      (with-redefs [jdbc/execute-one! (fn [_ds _sql]
                                       (throw (ex-info "Database unavailable" {})))]
        (let [response (request handler "/ready")]
          (is (= 503 (:status response)))
          (is (= {:status "not-ready"
                  :error "db"}
                 (json-body response))))))))
