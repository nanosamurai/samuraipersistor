# AGENTS.MD — samuraipersistor (Clojure) Architecture + House Rules

This repository is a **Clojure microservice** whose responsibility is to persist
speech-to-text transcription events into a **shared Postgres DB**.

It has replaced DB persistence implemented inside individual
transcription workers (e.g. `whisperx_worker`, `finalizer_worker`) in the Python
`xamurai` monorepo.

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

--- Other Rules / Best Practices
- You are usually being run from CLINE IDE plugin, which limits you to the given working repository. If you want to execute commands (i.e. run executables, make git commands) in other folders / repositories, the only way you can do so is via powershell `Set-Location`
    - e.g. `powershell -NoProfile -Command "Set-Location 'C:\Users\miros\IdeaProjects\samuraipersistor'; clojure -M -e '(println :ok)'"`
- All changes should be documented accordingly in the relevant .MD docs; before claiming task is coplete, always check docs for gaps as well as for a stale or no longer correct information and fix that
- For any task, create a dedicated branch (named imperatively) from master (unless told otherwise) and as you progress make small(er) atomic commits named imperatively
- For a task to be completed, there should be no security risks or concerns, always assess task's completion also from the security standpoint and highlight any risks.
- NEVER EXPOSE PORTS on local LAN for testing, always use localhost or 127.0.0.1
- Xamurai used to be called "drsynth", so do not be confused if you do see this name somewhere. That name however is legacy and you should always use xamurai when naming things related to xamurai stack- alternatively, you can also use nanosamurai if the name refers to the whole solution and not just the subset of voice-to-text services