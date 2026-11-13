(ns samuraipersistor.ce-feature-flags-test
  (:require [clojure.test :refer [deftest is testing]]
            [integrant.core :as ig]
            [samuraipersistor.features :as features]
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

(def ^:private lanes
  [:workflow-result-enabled? :workflow-outcome-enabled? :webhook-outcome-enabled?])

(deftest lane-flags-default-test
  (testing "unset lane flags are off in CE mode and when the CE switch is absent"
    (doseq [kcfg [{} {:ce-mode? true}]
            lane lanes]
      (is (false? (features/lane-enabled? kcfg lane)) [kcfg lane])))
  (testing "deprecated SAMURAIPERSISTOR_CE_MODE=false turns unset lane flags on"
    (doseq [lane lanes]
      (is (true? (features/lane-enabled? {:ce-mode? false} lane)) lane)))
  (testing "an explicit lane flag wins over the CE switch"
    (doseq [lane lanes]
      (is (true? (features/lane-enabled? {:ce-mode? true lane true} lane)) lane)
      (is (false? (features/lane-enabled? {:ce-mode? false lane false} lane)) lane))))

(deftest lane-env-overlay-test
  (testing "SP_*_ENABLED env vars parse into explicit lane flags"
    (with-redefs-fn {#'system/env-value (fn [k & [parser]]
                                         (case k
                                           "SP_WORKFLOW_RESULT_ENABLED" (parser "true")
                                           "SP_WEBHOOK_OUTCOME_ENABLED" (parser "false")
                                           nil))}
      (fn []
        (let [kcfg (get-in (#'system/apply-env-overrides {:samuraipersistor/config {:kafka {}}})
                           [:samuraipersistor/config :kafka])]
          (is (true? (:ce-mode? kcfg)))
          (is (true? (features/lane-enabled? kcfg :workflow-result-enabled?)))
          (is (false? (features/lane-enabled? kcfg :workflow-outcome-enabled?)))
          (is (false? (features/lane-enabled? kcfg :webhook-outcome-enabled?))))))))

(deftest ce-mode-lane-flags-start-only-their-consumer-test
  (let [lane->consumer {:workflow-result-enabled? [:samuraipersistor/workflow-result-consumer "workflow.result"]
                        :workflow-outcome-enabled? [:samuraipersistor/workflow-outcome-consumer "workflow.outcome"]
                        :webhook-outcome-enabled? [:samuraipersistor/webhook-outcome-consumer "webhook.delivery_outcome"]}
        base {:ce-mode? true
              :bootstrap-servers "localhost:9092"
              :webhook-outcome-consumer-group-id "webhook-group"
              :workflow-result-consumer-group-id "workflow-result-group"
              :workflow-outcome-consumer-group-id "workflow-outcome-group"
              :topics {:webhook-delivery-outcome "webhook.delivery_outcome"
                       :workflow-result "workflow.result"
                       :workflow-outcome "workflow.outcome"}}
        worker {:stop! (fn [])}]
    (doseq [[lane [_ topic]] lane->consumer]
      (testing (str "CE mode with only " (name lane) " set to true")
        (let [started (atom [])
              config {:kafka (assoc base lane true)}]
          (with-redefs-fn {#'consumer-loop/start-consumer-loop! (fn [opts]
                                                                  (swap! started conj opts)
                                                                  {:queue ::queue
                                                                   :commit! (fn [_])
                                                                   :stop! (fn [])})
                           #'webhook-outcome/start-worker! (fn [_] worker)
                           #'workflow-result/start-worker! (fn [_] worker)
                           #'workflow-outcome/start-worker! (fn [_] worker)}
            (fn []
              (doseq [[other-lane [k _]] lane->consumer]
                (is (= (= other-lane lane)
                       (:enabled? (ig/init-key k {:config config :db {}})))
                    (name k)))
              (is (= [topic] (mapv :topic @started))))))))))
