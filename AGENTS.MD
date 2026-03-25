# AGENTS.MD — samuraipersistor (Clojure) Architecture + House Rules

This repository is a **Clojure microservice** whose responsibility is to persist
speech-to-text transcription events into a **shared Postgres DB**.

It has replaced DB persistence implemented inside individual
transcription workers (e.g. `whisperx_worker`, `finalizer_worker`) in the Python
`xamurai` monorepo.

## Main Rules / Best Practices
- You are usually being run from CLINE IDE plugin, which limits you to the given working repository. If you want to execute commands (i.e. run executables, make git commands) in other folders / repositories, the only way you can do so is via powershell `Set-Location`
    - e.g. `powershell -NoProfile -Command "Set-Location 'C:\Users\miros\IdeaProjects\samuraipersistor'; clojure -M -e '(println :ok)'"`
- All changes should be documented accordingly in the relevant .MD docs; before claiming task is coplete, always check docs for gaps as well as for a stale or no longer correct information and fix that
- For any task, create a dedicated branch (named imperatively) from master (unless told otherwise) and as you progress make small(er) atomic commits named imperatively
- For a task to be completed, there should be no security risks or concerns, always assess task's completion also from the security standpoint and highlight any risks.
- NEVER EXPOSE PORTS on local LAN for testing, always use localhost or 127.0.0.1
- Xamurai used to be called "drsynth", so do not be confused if you do see this name somewhere. That name however is legacy and you should always use xamurai when naming things related to xamurai stack- alternatively, you can also use nanosamurai if the name refers to the whole solution and not just the subset of voice-to-text services
- before you claim any task is complete, run full set of test/integration tests
- DO NOT EVER attempt to read or fetch the compiled js code (usually at resources/public/js/main.js ) - it is too big and your cotext will rot into oblivion
- DO NOT EVER run `clojure -Spath` or similar command to list entire classpath, you would burn too many tokens and rot your context without any significant benefit
- Remember that if you have issues formatting clojure, you should use the clojure mcp server running locally to validate any expression, test new functions etc. - or you can run clojure tool locally and execute also any code to see how it fares.

# About Nanosamur.ai
Nanosamurai solution currently consists of
- **SamuraiBFF** (https://github.com/mikub/samuraibff locally usually at `%USERPROFILE%\IdeaProjects\samuraibff`),
- **xamurai** (https://github.com/mikub/xamurai ; locally usually at `%USERPROFILE%\PycharmProjects\xamurai`) services connected via Kafka/gRPC
    - xamurai is a monorepo that contains multiple Speach-to-text and speaker diarization services for Nanosamur.ai solution - they provide both real-time transcription as well as near-real-time refinement.
- **samuraipersistor** (this repo) (https://github.com/mikub/samuraipersistor ; locally usually at `%USERPROFILE%\IdeaProjects\samuraipersistor` ) for persistence.
    - Database persistence is handled by a separate service, the samuraipersistor; consumes from all relevant Kafka topics and stores the data in Postgres
- **nanodeploy** - Helm charts for local k8s  and aws EKS deployments and also local docker compose setup (https://github.com/mikub/nanodeploy ; locally usually at `%USERPROFILE%\PycharmProjects\nanodeploy`)
- **nanplatform** - Pulumi infra setup in AWS (https://github.com/mikub/nanplatform ; locally usually at `%USERPROFILE%\PycharmProjects\nanplatform`)
- **nanosamurai-sdk** - A python sdk client for nanosamur.ai (uses client credentials for m2m access) (https://github.com/mikub/nanosamurai-sdk ; locally usually at `%USERPROFILE%\PycharmProjects\nanosamurai-sdk`)


## 1) Samurai Persistor Service responsibilities

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
