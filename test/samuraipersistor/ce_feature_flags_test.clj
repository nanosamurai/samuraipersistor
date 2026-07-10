(ns samuraipersistor.ce-feature-flags-test
  (:require [clojure.test :refer [deftest is testing]]
            [integrant.core :as ig]
            [samuraipersistor.kafka.consumer-loop :as consumer-loop]
            [samuraipersistor.system :as system]
            [samuraipersistor.webhook-outcome-consumer :as webhook-outcome]
            [samuraipersistor.workflow-outcome-consumer :as workflow-outcome]
            [samuraipersistor.workflow-result-consumer :as workflow-result]))

(deftest ce-mode-env-overlay-test
  (testing "CE mode defaults to true"
    (with-redefs-fn {#'system/env-value (fn [_ & _] nil)}
      (fn []
        (let [cfg' (#'system/apply-env-overrides {:samuraipersistor/config {:kafka {}}})]
          (is (= true (get-in cfg' [:samuraipersistor/config :kafka :ce-mode?])))))))

  (testing "SAMURAIPERSISTOR_CE_MODE=false disables CE mode"
    (with-redefs-fn {#'system/env-value (fn [k & [parser]]
                                         (case k
                                           "SAMURAIPERSISTOR_CE_MODE" (parser "false")
                                           nil))}
      (fn []
        (let [cfg' (#'system/apply-env-overrides {:samuraipersistor/config {:kafka {}}})]
          (is (= false (get-in cfg' [:samuraipersistor/config :kafka :ce-mode?]))))))))

(deftest ce-mode-consumers-noop-without-commercial-topics-test
  (testing "default CE mode does not validate workflow/webhook topics"
    (let [config {:kafka {:ce-mode? true
                          :topics {}}}]
      (is (= {:enabled? false}
             (ig/init-key :samuraipersistor/webhook-outcome-consumer {:config config :db {}})))
      (is (= {:enabled? false}
             (ig/init-key :samuraipersistor/workflow-result-consumer {:config config :db {}})))
      (is (= {:enabled? false}
             (ig/init-key :samuraipersistor/workflow-outcome-consumer {:config config :db {}}))))))

(deftest full-mode-consumers-start-by-default-test
  (testing "CE false and nil per-feature flags start workflow/webhook consumers"
    (let [started (atom [])
          worker {:stop! (fn [])}
          config {:kafka {:ce-mode? false
                          :bootstrap-servers "localhost:9092"
                          :webhook-outcome-consumer-group-id "webhook-group"
                          :workflow-result-consumer-group-id "workflow-result-group"
                          :workflow-outcome-consumer-group-id "workflow-outcome-group"
                          :topics {:webhook-delivery-outcome "webhook.delivery_outcome"
                                   :workflow-result "workflow.result"
                                   :workflow-outcome "workflow.outcome"}}}]
      (with-redefs-fn {#'consumer-loop/start-consumer-loop! (fn [opts]
                                                              (swap! started conj opts)
                                                              {:queue ::queue
                                                               :commit! (fn [_])
                                                               :stop! (fn [])})
                       #'webhook-outcome/start-worker! (fn [_] worker)
                       #'workflow-result/start-worker! (fn [_] worker)
                       #'workflow-outcome/start-worker! (fn [_] worker)}
        (fn []
          (is (= true (:enabled? (ig/init-key :samuraipersistor/webhook-outcome-consumer {:config config :db {}}))))
          (is (= true (:enabled? (ig/init-key :samuraipersistor/workflow-result-consumer {:config config :db {}}))))
          (is (= true (:enabled? (ig/init-key :samuraipersistor/workflow-outcome-consumer {:config config :db {}}))))
          (is (= ["webhook.delivery_outcome" "workflow.result" "workflow.outcome"]
                 (mapv :topic @started))))))))

(deftest per-feature-flags-disable-outside-ce-test
  (testing "per-feature false flags return no-op state before topic validation"
    (let [config {:kafka {:ce-mode? false
                          :webhook-outcome-enabled? false
                          :workflow-result-enabled? false
                          :workflow-outcome-enabled? false
                          :topics {}}}]
      (is (= {:enabled? false}
             (ig/init-key :samuraipersistor/webhook-outcome-consumer {:config config :db {}})))
      (is (= {:enabled? false}
             (ig/init-key :samuraipersistor/workflow-result-consumer {:config config :db {}})))
      (is (= {:enabled? false}
             (ig/init-key :samuraipersistor/workflow-outcome-consumer {:config config :db {}}))))))
