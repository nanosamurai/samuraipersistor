# samuraipersistor

Kafka → Postgres persistence service for drsynth transcript events.

This service is intended to replace direct DB persistence currently implemented inside
Python workers (`whisperx_worker`, `finalizer_worker`) in the `drsynth` monorepo.

## Responsibilities

### Consumes (Kafka)

* `transcripts.refined` (`RefinedEvent` protobuf)
* `transcripts.final` (`SessionTranscript` protobuf)

Messages **must be keyed by `session_id`** (session business key / `sessions.session_key`).

### Persists (Postgres)

Persists into the shared `drsynth` schema tables:

* `sessions` (assumed to be created by BFF)
* `recordings`
* `session_transcripts` (append-only transcript records)

### Optional DLQ (Kafka)

If persistence cannot proceed (most importantly: missing `sessions.session_key`), the event is sent to:

* `transcripts.persistor.dlq` (JSON payload)

and the Kafka offset is committed (so the consumer group does not get stuck).

## Architecture

* **Lifecycle:** Integrant
* **DB:** HikariCP + next.jdbc, prepared statements
* **Kafka:** Java `kafka-clients`
* **Backpressure:** poll loop feeds a bounded queue; when full the consumer pauses partitions
* **Commit safety:** KafkaConsumer is not thread-safe; offsets are committed **only from polling thread** (workers enqueue commit requests)

## DB migrations ownership

This repo does **not** own DB migrations.

For now the canonical migrations live in `drsynth/migrations`.
Running this service requires the shared DB schema to include the append-only transcript record changes (drsynth migration `0002_transcript_records_append_only`).

## Configuration

System config is in `resources/system.edn`.

Defaults currently point to the **real shared infra ports**:

* Postgres: `localhost:5432`
* Kafka: `localhost:9092`

For local development and tests, prefer **Testcontainers** (see tests below) and avoid touching shared infra.

## Running

Start the service (HTTP + Kafka consumers):

```bash
clojure -M:run
```

Health endpoints:

* `GET /health`
* `GET /ready` (currently checks DB connectivity)

## Logging

Logging is configured via `resources/log4j2.xml`.

Security note: we do **not** log transcript text at INFO by default.

## Tests

Integration tests use **Testcontainers Postgres** and run on fixed ports to avoid clashing with real services:

* Postgres container exposed on `localhost:15432`

Run all tests:

```bash
clojure -X:test
```

## Notes / known follow-ups

* Add Kafka Testcontainers tests (produce protobuf events, assert DB writes + commits).
* Carry `window_length`, `model`, `source` via Kafka headers (or a future envelope message).
* Consider adding de-duplication using a unique `(topic, partition, offset)` key in DB.
