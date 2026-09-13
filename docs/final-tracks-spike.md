# Final track spike validation

Validated on 2026-09-13 on `implement-lean-final-tracks`.

- `TESTCONTAINERS_RYUK_DISABLED=true clojure -X:test`: 14 tests, 73 assertions,
  zero failures/errors, including real Postgres concurrent recording reuse,
  duplicate final delivery, first-result preservation and tenant mismatch.
- Changed Clojure namespaces passed lint and compilation; protobuf Java bindings
  were regenerated with protoc 33.4.
- Rebuilt `samuraipersistor:lean-tracks`; the full Nanosamurai Compose smoke
  passed against real Kafka and Postgres. It verified separate final track/model
  rows sharing one recording, duplicate input/output replay, and recovery from a
  session-specific DB fault without restart or committing the failed event.
- Compose also passed real WhisperX, peer worker failure/restart, silence,
  selection/skip, tenant denial, BFF playback and source/sidecar cleanup checks.

Postgres/Kafka test ports bind only to localhost. Retry logs contain exception
class and SQL state rather than PostgreSQL's potentially sensitive failing row.
Build contexts exclude credentials and scratch files.

Migrations are owned by Nanodeploy and Nanosamurai; historical Migratus files
here are not the deployment ledger. Keep their migration 017/018 SQL copies in
sync. The repeatable overlay and smoke runbook are in Nanosamurai's
`docs/final-tracks-spike.md`, mirrored in Nanodeploy.

The first Compose pass used a separate validation DB and separate Persistor
groups. After explicit approval, migration 018 and a separate local cleanup
removed the retained DB's obsolete experimental schema, preserving all original
transcript/recording content. The full Compose smoke passed again against that
retained DB with the rebuilt image and original Persistor groups. Refinement
tracks, browser tabs and durable per-track failure reporting are later work.
