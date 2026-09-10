# Opt-in final-track persistence

Set `SP_FINAL_TRACKS_ENABLED=true` after applying deployment migration
`014-final-track-results.up.sql`. Production migration ownership stays in
`nanodeploy` and `nanosamurai`; `test-resources/final_track_schema.sql` is only
the integration fixture mirror. Set `SP_FINAL_SOURCE_BUCKET` to the recorder's
bucket and, if changed, `SP_FINAL_RECORDING_PREFIX` to its source prefix
(default `recordings`).

The canonical `FinalTrackResult` consumer reads `transcripts.final-tracks` in
group `samuraipersistor-final-tracks`. It indexes outcome identity, status,
artifact references, capabilities, degradations and provenance in
`transcript_track_results`. Detailed words remain in the immutable S3 transcript
artifact. It checks deterministic run/result identity, exact source/result
storage scope and the tenant-owned session's frozen track selection.

Both canonical outcomes and primary projections upsert the same recording by
`(session_id, source_artifact_id)`. Reusing an identity with different metadata
is rejected. Primary `SessionTranscript` messages retain their existing body
and carry additive result/source headers. `session_transcripts.result_id` makes
those inserts idempotent. Canonical-first and primary-first delivery both work;
no cross-topic ordering is assumed. Duplicate event digests must match exactly.
Unidentified legacy final messages retain their append-only behavior.

In opt-in mode, both final consumers process one pending record per poll loop.
SQL transactions have a 30-second statement timeout. Database failures retry
that same record without advancing the offset; rebalances release local
ownership. SQL commit (or an acknowledged metadata-only DLQ for permanent
contract/missing-session errors) precedes the Kafka input commit. There is no
exactly-once delivery claim. Transcript bodies, audio locations and raw rejected
payloads are not included in the new consumer's logs or DLQ records.

The feature defaults off and starts in Community Edition mode when enabled.
The existing refined lane is unchanged. This is a local spike, not production
promotion: shared source/artifact cleanup, broader failure scheduling, a second
real profile and windowed refinement remain later work. The standalone
`nanosamurai` Compose harness verifies real Kafka, S3 and PostgreSQL behavior.

Tests include a synthetic Python-generated protobuf/header vector, two SQL
arrival orders, exact replay, conflicting duplicates, legacy appends and
tenant/storage tampering. Run the full suite with
`TESTCONTAINERS_RYUK_DISABLED=true`; owned PostgreSQL and Kafka fixtures bind
explicitly to `127.0.0.1` and stop in `finally`.
