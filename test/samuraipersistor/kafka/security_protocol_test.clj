(ns samuraipersistor.kafka.security-protocol-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [samuraipersistor.system :as system]
    [samuraipersistor.kafka.common :as kcommon]
    [samuraipersistor.refined-consumer :as refined-consumer]
    [samuraipersistor.final-consumer :as final-consumer]))

(deftest env-overlay-reads-kafka-security-protocol-test
  (testing "apply-env-overrides reads SP_KAFKA_SECURITY_PROTOCOL into config"
    (with-redefs-fn {#'system/env-value (fn [k & [_parser]]
                                         (case k
                                           "SP_KAFKA_SECURITY_PROTOCOL" "SSL"
                                           nil))}
      (fn []
        (let [cfg {:samuraipersistor/config {:kafka {}}}
              cfg' (#'system/apply-env-overrides cfg)]
          (is (= "SSL" (get-in cfg' [:samuraipersistor/config :kafka :security-protocol])))))))

  (testing "apply-env-overrides falls back to generic KAFKA_SECURITY_PROTOCOL"
    (with-redefs-fn {#'system/env-value (fn [k & [_parser]]
                                         (case k
                                           "KAFKA_SECURITY_PROTOCOL" "SSL"
                                           nil))}
      (fn []
        (let [cfg {:samuraipersistor/config {:kafka {}}}
              cfg' (#'system/apply-env-overrides cfg)]
          (is (= "SSL" (get-in cfg' [:samuraipersistor/config :kafka :security-protocol]))))))))

(deftest kafka-config-maps-include-security-protocol-test
  (testing "DLQ producer config includes security.protocol"
    (is (= "SSL" (get (#'kcommon/producer-config {:bootstrap-servers "x"
                                                   :security-protocol "SSL"})
                      "security.protocol"))))

  (testing "refined consumer-config includes security.protocol"
    (is (= "SSL" (get (#'refined-consumer/consumer-config {:bootstrap-servers "x"
                                                            :refined-consumer-group-id "g"
                                                            :security-protocol "SSL"})
                      "security.protocol"))))

  (testing "final consumer-config includes security.protocol"
    (is (= "SSL" (get (#'final-consumer/consumer-config {:bootstrap-servers "x"
                                                          :final-consumer-group-id "g"
                                                          :security-protocol "SSL"})
                      "security.protocol")))))
