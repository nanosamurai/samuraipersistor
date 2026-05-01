-- migrations/0005_workflow_outcomes.up.sql
-- Persist RFC-0003 workflow runner attempt outcomes (audit lane).
--
-- Notes:
-- - Append-only history table: workflow_outcomes
-- - No FK to sessions: audit lane must not block on missing session rows.

CREATE TABLE workflow_outcomes (
    id              uuid PRIMARY KEY,
    created_at      timestamptz NOT NULL,

    workflow_run_id uuid NOT NULL,
    tenant_id       uuid NOT NULL,
    session_id      uuid NOT NULL,
    workflow_id     uuid NOT NULL,

    attempt_no      integer NOT NULL,
    status          text NOT NULL,
    latency_ms      bigint NULL,
    retry_to_topic  text NULL,
    error_code      text NULL,
    error_detail    text NULL,

    kafka_topic     text NULL,
    kafka_partition integer NULL,
    kafka_offset    bigint NULL
);

-- Idempotency under Kafka redeliveries.
ALTER TABLE workflow_outcomes
    ADD CONSTRAINT workflow_outcomes_run_attempt_uniq
    UNIQUE (workflow_run_id, attempt_no);

CREATE INDEX idx_workflow_outcomes_tenant_created_at
    ON workflow_outcomes (tenant_id, created_at DESC);

CREATE INDEX idx_workflow_outcomes_session_created_at
    ON workflow_outcomes (session_id, created_at DESC);
