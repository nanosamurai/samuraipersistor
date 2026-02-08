# AGENTS.MD — samuraipersistor (Clojure) Architecture + House Rules

This repository is a **Clojure microservice** whose responsibility is to persist
speech-to-text transcription events into a **shared Postgres DB**.

It is intended to replace DB persistence currently implemented inside individual
transcription workers (e.g. `whisperx_worker`, `finalizer_worker`) in the Python
`drsynth` monorepo.

## 1) Service responsibilities

### Inputs
Consumes Kafka topics (protobuf payloads, keyed by `session_id`):

* `transcripts.refined` — `RefinedEvent`
* `transcripts.final` — `SessionTranscript`

### Outputs

* Persists into Postgres tables owned by the shared `drsynth` schema:
  * `sessions` (assumed to be created by BFF)
  * `recordings`
  * `session_transcripts` (append-only)

* Optionally emits to a DLQ Kafka topic (JSON payload) when persistence cannot
  proceed (e.g. missing session):
  * `transcripts.persistor.dlq`

### Non-goals (for now)

* No query API.
* No auth.
* No schema migrations in this repo.

## 2) Architectural decisions

### Append-only transcript records
We keep `session_transcripts` as an append-only table (per drsynth migration 0002).
This supports multiple transcription worker variants (different models/windows).

### Session ownership
The **BFF owns session creation**. Persistor treats missing sessions as an error
and should send the event to DLQ + commit the Kafka offset (so the consumer group
does not get stuck).

### Kafka ordering / partitioning assumptions
Workers MUST publish messages keyed by `session_id`.
This ensures per-session ordering within a topic.
Cross-topic ordering (`refined` vs `final`) is not guaranteed and not required.

### Backpressure
Kafka poll loop uses a bounded queue. When the queue is full, the consumer pauses
assigned partitions until the queue has space.

### Offset commits
KafkaConsumer is **not thread-safe**.
Offsets are committed **only from the polling thread**. Worker threads enqueue
commit requests.

## 3) Tech stack (locked in)

* Lifecycle: **Integrant**
* Concurrency: threads + bounded queues (simple, predictable)
* Kafka: `org.apache.kafka/kafka-clients`
* Postgres: `next.jdbc` + **HikariCP**
* Logging: `org.corfield.logging4j2` (Log4j2 backend)
* Integration testing: **Testcontainers** (Postgres and later Kafka)
* Protobuf: Java generated stubs vendored in `src/java/samuraibff/proto/*`

## 4) Engineering house rules

### Documentation
* Every public function should have a docstring that states:
  * purpose
  * inputs/outputs (types/shapes)
  * error behavior

### Logging
* Never use `println` for logs.
* Prefer structured logging maps with keys like:
  * `:session-id`, `:tenant-id`
  * `:topic`, `:partition`, `:offset`
  * `:source`, `:type`, `:model`, `:window-length`
* Never log transcript text at INFO by default (security/PII).

### DB
* Use prepared statements.
* Prefer batched inserts where possible.
* Use connection pooling (Hikari).

### Testing
* New persistence logic must have integration tests (Postgres Testcontainers).
* Kafka-related logic must have integration tests (Kafka Testcontainers) before
  we consider it production-ready.

### Security / OWASP-ish defaults
* Config (DB creds, Kafka auth) via environment variables / secrets.
* TLS for Postgres and Kafka in production.
* Health endpoints should be network-restricted at the platform level.
