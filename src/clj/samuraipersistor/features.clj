(ns samuraipersistor.features
  "Optional Kafka lanes and their flags.

  Each optional lane has its own flag under `:kafka`:

  - `:workflow-result-enabled?` (SP_WORKFLOW_RESULT_ENABLED): store workflow
    results from `workflow.result`.
  - `:workflow-outcome-enabled?` (SP_WORKFLOW_OUTCOME_ENABLED): store workflow
    run outcomes from `workflow.outcome`.
  - `:webhook-outcome-enabled?` (SP_WEBHOOK_OUTCOME_ENABLED): store webhook
    delivery outcomes.

  An unset lane flag defaults to false. The deprecated `:ce-mode?`
  (SAMURAIPERSISTOR_CE_MODE) only changes that default: when it is false,
  unset lane flags default to true. An explicit lane flag always wins.")

(defn lane-enabled?
  "Return true when the optional Kafka lane flag `flag-key` is on.

  Inputs:
  - kafka-config: the `:kafka` config map
  - flag-key: one of :workflow-result-enabled?, :workflow-outcome-enabled?
    or :webhook-outcome-enabled?

  Returns: boolean."
  [kafka-config flag-key]
  (let [v (get kafka-config flag-key)]
    (if (boolean? v)
      v
      (false? (:ce-mode? kafka-config)))))
