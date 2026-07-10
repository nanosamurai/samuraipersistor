# Community Edition feature flags

Status: planning

Last updated: 2026-07-10

## Decision

Community Edition should persist the STT data path by default and avoid starting
workflow/webhook persistence consumers unless the deployment explicitly opts
into the full commercial feature set.

Use a single edition flag first:

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `SAMURAIPERSISTOR_CE_MODE` | `true` | When true, do not start workflow/webhook outcome consumers. |

This keeps OSS easy to run: the `nanosamurai` Compose stack can omit the flag
and get CE behavior. Commercial/full deployments managed by `nanodeploy` should
set `SAMURAIPERSISTOR_CE_MODE=false`.

`SP_WEBHOOK_OUTCOME_ENABLED` already exists. Keep it as a per-feature override,
but make CE mode the higher-level default so OSS does not need to know about
commercial-only topics.

## What CE mode should affect

- Consumers:
  - do not start `:samuraipersistor/webhook-outcome-consumer`
  - do not start `:samuraipersistor/workflow-result-consumer`
  - do not start `:samuraipersistor/workflow-outcome-consumer`
- Kafka config:
  - do not require `webhook.delivery_outcome`, `workflow.result`, or
    `workflow.outcome` topics in CE
  - avoid noisy warnings when those topics are absent in the OSS stack
- Persistence:
  - leave workflow/webhook tables and functions in source; contract visibility
    is acceptable
  - do not write workflow/webhook outcome rows in CE because the consumers are
    not running
- STT path:
  - keep existing transcript/session persistence behavior unchanged

## Implementation plan

1. Add config parsing for `SAMURAIPERSISTOR_CE_MODE`, defaulting to true.
2. Define effective consumer flags:
   - webhook outcome enabled only when CE mode is false and
     `SP_WEBHOOK_OUTCOME_ENABLED` is not false
   - add equivalent `SP_WORKFLOW_RESULT_ENABLED` and
     `SP_WORKFLOW_OUTCOME_ENABLED` flags, defaulting to true only outside CE
3. Make disabled consumers return a small no-op state during Integrant init and
   log one concise message.
4. Ensure disabled consumers do not validate or require their Kafka topics.
5. Add focused tests for default CE startup and full-mode startup.
6. After code lands, update `nanosamurai` docs to rely on the default CE mode
   and update `nanodeploy` k8s/Compose values to set
   `SAMURAIPERSISTOR_CE_MODE=false`.

## Security note

This flag is primarily for product boundary, UX clarity, and dependency
simplification. It does not need to hide schemas or persistence code. The
important behavior is that CE does not subscribe to workflow/webhook topics or
require those services by default.
