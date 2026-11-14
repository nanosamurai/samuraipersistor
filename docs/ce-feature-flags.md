# Feature flags

Status: implemented

Last updated: 2026-11-13

## Decision

The persistor always stores the STT data path (sessions, refined and final
transcripts). Each optional Kafka lane has its own flag, so a deployment turns
on the lanes it runs instead of choosing an edition:

| Environment variable | Default | Starts |
| --- | --- | --- |
| `SP_WORKFLOW_RESULT_ENABLED` | `false` | `:samuraipersistor/workflow-result-consumer`: stores `workflow.result` into `workflow_results_latest`. Needed for on-demand and automatic workflow runs. |
| `SP_WORKFLOW_OUTCOME_ENABLED` | `false` | `:samuraipersistor/workflow-outcome-consumer`: stores the `workflow.outcome` audit lane. |
| `SP_WEBHOOK_OUTCOME_ENABLED` | `false` | `:samuraipersistor/webhook-outcome-consumer`: stores `webhook.delivery_outcome` from a webhook delivery service. |
| `SAMURAIPERSISTOR_CE_MODE` | `true` | Deprecated. When `false`, every lane flag above that is not set defaults to `true`. |

`1`, `true`, `yes` and `on` (also upper case) turn a flag on. Any other
non-blank value turns it off.

An explicit lane flag always wins over `SAMURAIPERSISTOR_CE_MODE`. For example,
a Community Edition stack with on-demand workflows sets
`SP_WORKFLOW_RESULT_ENABLED=true` and `SP_WORKFLOW_OUTCOME_ENABLED=true` and
leaves webhooks off; it no longer needs `SAMURAIPERSISTOR_CE_MODE=false`.

## Upgrading from `SAMURAIPERSISTOR_CE_MODE`

* Nothing set: every lane stays off, as before.
* `SAMURAIPERSISTOR_CE_MODE=false`: every lane stays on unless its flag is
  set to off, as before. Replace the switch with the lane flags set to `true`
  when convenient; it will be removed in a later release.
* A lane flag set to `true` with CE mode on: before, the persistor ignored it;
  now it starts that consumer.

## What a disabled lane does

- Its consumer returns a small no-op state during Integrant init and logs one
  concise message.
- It does not validate or require its Kafka topic, so a stack without that
  topic starts cleanly.
- Workflow/webhook tables and functions stay in source; nothing writes to them
  while the lane is off.
- The STT path is unchanged.

## Implementation notes

- Flags are parsed into `[:kafka :workflow-result-enabled?]`,
  `[:kafka :workflow-outcome-enabled?]`, `[:kafka :webhook-outcome-enabled?]`
  and the deprecated `[:kafka :ce-mode?]` (default `true`).
- `samuraipersistor.features/lane-enabled?` is the only place that resolves
  them.

## Security note

The flags set the product boundary and keep unused dependencies off. They do
not hide schemas or persistence code. Every lane is off unless set, so a
missing or misspelled value fails closed. A consumer stores what arrives on its
topic, so turn a lane on only when a trusted producer writes to that topic and
Kafka ACLs restrict who may write.
