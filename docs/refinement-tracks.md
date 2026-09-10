# Refinement track persistence

Apply deployment migration `015-refinement-track-results.up.sql`, then set
`SP_REFINEMENT_TRACKS_ENABLED=true` and `SP_FINAL_SOURCE_BUCKET` to the shared
source bucket. This independently enables the canonical refinement consumer
in Community Edition; final-track opt-in remains a separate flag.

Group `samuraipersistor-refined-tracks` reads `transcripts.refined-tracks` using
the existing bounded `FinalTrackResult` envelope with `stage=refined`. It checks
tenant ownership, the frozen stage catalog and window policy, deterministic
run/unit/result identity, and source/artifact scope. Canonical metadata goes to
`transcript_track_results`; window source metadata uses its new `source` JSONB
column. Window outcomes do not create recording rows or complete the session.

Successful primary `RefinedEvent` messages carry their accepted outcome in
`x-track-outcome`. Primary-first and canonical-first delivery use the same
transactional validation and canonical row. Stable `result_id` and exact event
digests deduplicate `session_transcripts`. The existing recordings API therefore
receives only primary windows; secondary results remain independently indexed.
Legacy messages without track identity keep their existing behavior.

Both opted-in refinement consumers share final tracks' poll-thread retry and
commit discipline: SQL commit or an acknowledged metadata-only permanent-error
DLQ precedes Kafka commit. Database failures retry the pending record. A session
row lock serializes frozen-plan/source checks across primary and canonical
consumers; no cross-topic ordering is required. Logs/DLQ omit transcript bodies.

The SQL integration fixtures cover two windows, two tracks, both arrival orders,
exact replay and tenant/identity conflicts. Run `clojure -X:test` with
`TESTCONTAINERS_RYUK_DISABLED=true`; fixtures bind to `127.0.0.1`. Source deletion,
artifact retention and track-aware workflow views remain separate work.
