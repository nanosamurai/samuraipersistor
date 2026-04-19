# samuraipersistor

Kafka → Postgres persistence service for nanosamurai's transcript events.

This service is intended to replace direct DB persistence currently implemented inside
Python workers (`whisperx_worker`, `finalizer_worker`) in the `xamurai` monorepo (formerly 'drsynth').

## About Nanosamur.ai
Nanosamurai solution currently consists of
- **SamuraiBFF** - the BFF for the solution (https://github.com/mikub/samuraibff ; locally usually at `%USERPROFILE%\IdeaProjects\samuraibff`),
- **xamurai** (https://github.com/mikub/xamurai ; locally usually at `%USERPROFILE%\PycharmProjects\xamurai`) services connected via Kafka/gRPC
    - xamurai is a monorepo that contains multiple Speach-to-text and speaker diarization services for Nanosamur.ai solution - they provide both real-time transcription as well as near-real-time refinement.
- **samuraipersistor** (this repo)
---

## Responsibilities of samuraipersistor

### Consumes (Kafka)

* `transcripts.refined` (`RefinedEvent` protobuf)
* `transcripts.final` (`SessionTranscript` protobuf)

Optional audit lane:

* `webhook.delivery_outcome` (JSON, emitted by `webhook-dispatcher`)

Messages **must be keyed by `session_id`** (session business key / `sessions.session_key`).

### Persists (Postgres)

Persists into the shared `drsynth` schema tables:

* `sessions` (assumed to be created by BFF)
* `recordings`
* `session_transcripts` (append-only transcript records)

Optional audit/reporting tables:

* `webhook_delivery_outcomes` (append-only)
* `webhook_delivery_latest` (one row per `(tenant_id, webhook_id)` for fast UI list views)

`SessionTranscript.segments` persisted into `session_transcripts.segments` may optionally include word-level timing
information under `segments[].words[]` (when produced by the xamurai finalizer).

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

## DB migrations

Migrations are managed in this repo using **Migratus** (raw SQL `.up.sql` / `.down.sql`).

Migration files live in:

* `resources/migrations/*`

### Run migrations

```bash
clojure -X:migrate :jdbc-url 'jdbc:postgresql://localhost:5432/drsynth' :username 'drsynth' :password 'drsynth'
```

### Rollback last migration

```bash
clojure -X:rollback :jdbc-url 'jdbc:postgresql://localhost:5432/drsynth' :username 'drsynth' :password 'drsynth'
```

## Configuration

System config is in `resources/system.edn`.

For k8s/Docker deployments, configuration can be overridden via environment variables
prefixed with `SP_` (see table below).

Defaults currently point to the **real shared infra ports**:

* Postgres: `localhost:5432`
* Kafka: `localhost:9092`

For local development and tests, prefer **Testcontainers** (see tests below) and avoid touching shared infra.

### Environment variables

| Area | Variable | Purpose | Default (from `resources/system.edn`) |
| --- | --- | --- | --- |
| HTTP | `SP_HTTP_HOST` | HTTP bind host | `0.0.0.0` |
| HTTP | `SP_HTTP_PORT` | HTTP bind port | `8010` |
| DB | `SP_DB_JDBC_URL` | JDBC URL | `jdbc:postgresql://localhost:5432/drsynth` |
| DB | `SP_DB_USERNAME` | DB user | `drsynth` |
| DB | `SP_DB_PASSWORD` | DB password | `drsynth` |
| DB | `SP_DB_MAX_POOL_SIZE` | Hikari max pool size | `10` |
| DB | `SP_DB_MIN_IDLE` | Hikari min idle | `2` |
| DB | `SP_DB_CONN_TIMEOUT_MS` | Hikari connection timeout (ms) | `5000` |
| Kafka | `SP_KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `localhost:9092` |
| Kafka | `SP_KAFKA_CLIENT_ID` | Kafka client id | `samuraipersistor` |
| Kafka | `SP_KAFKA_REFINED_GROUP_ID` | Consumer group for refined | `samuraipersistor-refined` |
| Kafka | `SP_KAFKA_FINAL_GROUP_ID` | Consumer group for final | `samuraipersistor-final` |
| Kafka | `SP_KAFKA_TOPIC_REFINED` | Refined topic | `transcripts.refined` |
| Kafka | `SP_KAFKA_TOPIC_FINAL` | Final topic | `transcripts.final` |
| Kafka | `SP_KAFKA_TOPIC_WEBHOOK_DELIVERY_OUTCOME` | Webhook delivery outcome topic | `webhook.delivery_outcome` |
| Kafka | `SP_KAFKA_TOPIC_DLQ` | DLQ topic (optional) | `transcripts.persistor.dlq` |
| Kafka | `SP_KAFKA_MAX_POLL_RECORDS` | Max poll records | `200` |
| Kafka | `SP_KAFKA_MAX_POLL_INTERVAL_MS` | Max poll interval (ms) | `300000` |
| Kafka | `SP_KAFKA_SESSION_TIMEOUT_MS` | Session timeout (ms) | `30000` |
| Kafka | `SP_KAFKA_REFINED_BUFFER_SIZE` | Refined queue size | `2000` |
| Kafka | `SP_KAFKA_FINAL_BUFFER_SIZE` | Final queue size | `200` |
| Kafka | `SP_KAFKA_WEBHOOK_OUTCOME_GROUP_ID` | Consumer group for webhook outcomes | `samuraipersistor-webhook-outcomes` |
| Kafka | `SP_KAFKA_WEBHOOK_OUTCOME_BUFFER_SIZE` | Webhook outcomes queue size | `2000` |
| Feature flag | `SP_WEBHOOK_OUTCOME_ENABLED` | Enable webhook outcome consumer | `true` |

## Running

Start the service (HTTP + Kafka consumers):

```bash
clojure -M:run
```

## Docker / k8s

### Build image

```bash
docker build -t samuraipersistor:local .
```

### Run image (example)

```bash
docker run --rm -p 8010:8010 \
  -e SP_DB_JDBC_URL=jdbc:postgresql://host.docker.internal:5432/drsynth \
  -e SP_DB_USERNAME=drsynth \
  -e SP_DB_PASSWORD=drsynth \
  -e SP_KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  samuraipersistor:local
```

### k8s notes

- Configure secrets via env vars (see table above).
- Health endpoints for probes: `GET /health` and `GET /ready`.
- Do not expose health endpoints publicly; restrict at the platform level.

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

## CI

This repo uses **GitHub Actions** for CI. The PR gate runs:

```bash
clojure -T:build compile-java
clojure -X:test
```

Note: The test suite requires **Docker** (Testcontainers).

## Observability
The BFF supports OTEL integration and tracing - for more information see https://github.com/mikub/xamurai/blob/master/docs/local-k8s-observability.md (locally usually `%USERPROFILE%\PycharmProjects\xamurai\docs\local-k8s-observability.md`)

## Notes / known follow-ups

* Add Kafka Testcontainers tests (produce protobuf events, assert DB writes + commits).
* Carry `window_length`, `model`, `source` via Kafka headers (or a future envelope message).
* Consider adding de-duplication using a unique `(topic, partition, offset)` key in DB.

