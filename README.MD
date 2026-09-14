# samuraipersistor

Kafka → Postgres persistence service for nanosamurai's transcript events.

CI and full-history secret scanning also run for PRs targeting
`implement-lean-final-tracks`, so the stacked refinement spike receives the
same checks as a PR targeting `master`.

This service replaces direct DB persistence inside the Python workers
(`whisperx_worker`, `finalizer_worker`) in the `xamurai` monorepo.

## About Nanosamur.ai
The Community Edition consists of:

- **SamuraiBFF** (https://github.com/nanosamurai/samuraibff): API, browser UI,
  authentication, and orchestration.
- **xamurai** (https://github.com/nanosamurai/xamurai): speech-to-text and
  speaker-diarization workers connected through Kafka and gRPC.
- **samuraipersistor** (this repo): Kafka-to-Postgres persistence.
- **nanosamurai** (https://github.com/nanosamurai/nanosamurai): the public
  Docker Compose quickstart and operator guidance.
- **nanosamurai-sdk** (https://github.com/nanosamurai/nanosamurai-sdk): the
  Python client SDK.

---

## Responsibilities of samuraipersistor

### Consumes (Kafka)

* `transcripts.refined` (`RefinedEvent` protobuf)
* `transcripts.final` (`SessionTranscript` protobuf)

Optional audit lane:

* `webhook.delivery_outcome` (JSON, emitted by an external non-CE integration)

Workflows (RFC-0003):

* `workflow.result` (JSON, emitted by an external non-CE integration)
* `workflow.outcome` (JSON audit lane, emitted by an external non-CE integration)

  Note: although the v1 contract documents `created_at` as ISO-8601, in practice it may arrive as an **epoch numeric** timestamp (seconds, sometimes in scientific notation). Persistor accepts both formats.

Community Edition mode is enabled by default and starts only the STT transcript consumers.
Set `SAMURAIPERSISTOR_CE_MODE=false` for deployments that should also consume
workflow/webhook outcome topics.

Transcript messages **must be keyed by `session_id`** (session business key / `sessions.session_key`).

`webhook.delivery_outcome` messages are typically keyed by `webhook_id` (or `tenant_id`) depending on dispatcher configuration.

### Persists (Postgres)

Persists into the shared nanosamurai Postgres tables:

* `sessions` (assumed to be created by BFF)
* `recordings`
* `session_transcripts` (append-only transcript records)

Optional audit/reporting tables:

* `webhook_delivery_outcomes` (append-only)
* `webhook_delivery_latest` (one row per `(tenant_id, webhook_id)` for fast UI list views)

Workflows (RFC-0003) persistence tables:

* `workflow_results_history` (append-only history for non-incremental/final triggers)
* `workflow_results_latest` (one row per `(session_id, workflow_id)`; incremental workflows overwrite here)
* `workflow_outcomes` (append-only; idempotent on `(workflow_run_id, attempt_no)`)

Retention note (ops): `webhook_delivery_outcomes` can be high volume. Our target is **30 days** retention; implement cleanup via a DB job/cron (this repo does not schedule DB maintenance).

`SessionTranscript.segments` persisted into `session_transcripts.segments` may optionally include word-level timing
information under `segments[].words[]` (when produced by the xamurai finalizer).

`RefinedEvent.segments` is also persisted into `session_transcripts.segments` (JSONB). Newer refined producers may
emit multiple segments per refinement window (e.g. multiple speakers within a 60s slice). Older producers that do not
populate `RefinedEvent.segments` are handled in a backward-compatible way by persisting a single segment derived from
the legacy scalar fields (`start_s/end_s/text/speaker`).

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

Deployment migrations are owned by the deployment repositories: the Community
Edition Compose SQL in `nanosamurai/docker/postgres/migrations` and the matching
enterprise deployment SQL. Apply migration `017-add-final-track-identity.up.sql`
before this service. Keep those copies identical. This repository's historical
`resources/migrations` and Migratus commands are retained for compatibility;
they are not the current deployment migration source and must not be used to
migrate a running deployment.

The final protobuf adds only `SessionTranscript.track_id = 9`. An empty ID
maps to `whisperx`. The `model` Kafka header supplies the stored model label;
missing metadata is `unknown`, not a fabricated WhisperX model. The existing
transaction reuses the recording by `(session_id, recording_url)` and inserts
one final row per `(recording_id, track_id)`. Duplicate delivery does nothing;
the first text, segments and metadata remain unchanged. Old NULL track rows
remain historical. The final consumer retries a failed database write before
advancing to another event. Event tenant IDs must match the owning session.
Persistor publishes no transcript events.

Regenerate the checked-in Java messages with protoc 33.4:

```bash
protoc -I resources/proto --java_out=src/java resources/proto/stream.proto
clojure -T:build compile-java
```

Local test containers bind only to localhost. Set
`TESTCONTAINERS_RYUK_DISABLED=true` for local full-suite runs and allow test
fixtures to stop their containers. Docker builds exclude `.env` files and
scratch worktrees.
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
| Kafka | `SP_KAFKA_WORKFLOW_RESULT_GROUP_ID` | Consumer group for workflow results | `samuraipersistor-workflow-results` |
| Kafka | `SP_KAFKA_WORKFLOW_OUTCOME_GROUP_ID` | Consumer group for workflow outcomes | `samuraipersistor-workflow-outcomes` |
| Kafka | `SP_KAFKA_TOPIC_WORKFLOW_RESULT` | Workflow result topic | `workflow.result` |
| Kafka | `SP_KAFKA_TOPIC_WORKFLOW_OUTCOME` | Workflow outcome topic | `workflow.outcome` |
| Kafka | `SP_KAFKA_WORKFLOW_RESULT_BUFFER_SIZE` | Workflow result queue size | `2000` |
| Kafka | `SP_KAFKA_WORKFLOW_OUTCOME_BUFFER_SIZE` | Workflow outcome queue size | `2000` |
| Feature flag | `SAMURAIPERSISTOR_CE_MODE` | Community Edition mode; disables workflow/webhook consumers when true | `true` |
| Feature flag | `SP_WEBHOOK_OUTCOME_ENABLED` | Enable webhook outcome consumer outside CE mode | effectively `true` when `SAMURAIPERSISTOR_CE_MODE=false` |
| Feature flag | `SP_WORKFLOW_RESULT_ENABLED` | Enable workflow result consumer outside CE mode | effectively `true` when `SAMURAIPERSISTOR_CE_MODE=false` |
| Feature flag | `SP_WORKFLOW_OUTCOME_ENABLED` | Enable workflow outcome consumer outside CE mode | effectively `true` when `SAMURAIPERSISTOR_CE_MODE=false` |

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
docker run --rm -p 127.0.0.1:8010:8010 \
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

* `GET /health` is the liveness probe and returns HTTP 200 while the HTTP
  process is running. It does not check external dependencies.
* `GET /ready` is the readiness probe. It checks Postgres connectivity with a
  lightweight query, returning HTTP 200 when ready or HTTP 503 when the check
  fails.

Both endpoints return JSON (`application/json`) by default and when requested
with `Accept: application/json`. Clients can explicitly request EDN with
`Accept: application/edn` for backward compatibility. The endpoints are
unauthenticated; restrict access to them at the platform/network level.

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

This repo uses **GitHub Actions** for CI. Pull requests targeting `master` run:

```bash
clojure -T:build compile-java
clojure -X:test
```

Note: The test suite requires **Docker** (Testcontainers).

The pinned open-source Gitleaks CLI also scans the complete repository history
without injecting a repository secret. The `master` repository ruleset must
require these checks before merge:

* `Clojure tests (PR gate)`
* `scan`

The ruleset must require the pull request branch to be up to date and prevent
routine bypass. Workflow triggers make checks run; the repository ruleset
makes them merge prerequisites.

On pushes to `master`, the image publication workflow reuses the test and
Gitleaks workflows to validate the exact merged commit. Docker build, GHCR
login, and publication begin only after both gates succeed. The image is
published as
`ghcr.io/nanosamurai/samuraipersistor:sha-<git-sha>`.

Validation jobs are read-only. The publication job receives `packages: write`
and the GitHub attestation permissions; the Gitleaks gate does not require
`GITLEAKS_LICENSE`.
Repository secrets are unavailable to workflows triggered from forks; never
use `pull_request_target` to execute untrusted pull request code with secrets.

### Image SBOM

The published image includes an SPDX JSON software bill of materials as a
GHCR-attached OCI attestation. The workflow validates the SBOM after the push
and also uploads it as a workflow artifact named
`sbom-samuraipersistor-<git-sha>`. Public repositories additionally publish a
signed GitHub artifact attestation; that signing step is skipped while the
repository is private.

Retrieve the canonical SBOM attached to an image with:

```bash
docker buildx imagetools inspect \
  ghcr.io/nanosamurai/samuraipersistor:sha-<git-sha> \
  --format "{{ json .SBOM.SPDX }}" > samuraipersistor.spdx.json
```

## Observability
The BFF supports OpenTelemetry integration and tracing. For the Community Edition
stack, see the
[`nanosamurai` observability guide](https://github.com/nanosamurai/nanosamurai#observability).

## Notes / known follow-ups

* Refinement identity, replay deduplication and ordered database retry are
  qualified in [the refinement spike](docs/refinement-tracks-spike.md).
* Final tracks already use the existing `model` header and database uniqueness;
  no new message envelope or Kafka-offset database key is needed.
* See [final-track validation](docs/final-tracks-spike.md) for the real Compose
  Kafka/Postgres replay and database-retry evidence.


## Refinement track contract

Refinement tracks add only `RefinedEvent.track_id = 16`; an empty ID means
`whisperx`. Audio, segment and word message shapes and Kafka keys stay unchanged.


Apply deployment migration `019-add-refinement-track-identity.up.sql` before
running this version. It is owned by Nanosamurai and Nanodeploy, not this
repository's historical Migratus migrations. Keep their SQL copies identical.

Refined rows use the existing `track_id` column and `type='refined'`. Empty
protobuf IDs map to WhisperX; historical NULL rows stay untouched. The first
row per tenant/session/track/window length/start/end wins, including concurrent
and replayed deliveries. Model metadata comes from `RefinedEvent.refinement_model`,
not a hardcoded worker name. Segment and word JSON retain their original shape
and session-relative timing. Event tenant IDs must match the owning session.

The refined consumer retries failed writes before taking another record, then
requests commits through the existing polling thread. Missing/foreign sessions
use the existing DLQ path; transcript events are never republished. See
`docs/refinement-tracks-spike.md` for the database and Compose qualification.
