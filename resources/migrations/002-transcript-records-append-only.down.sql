-- migrations/0002_transcript_records_append_only.down.sql

DROP INDEX idx_session_transcripts_event_created_at_ns;
DROP INDEX idx_session_transcripts_session_type_source_window;
DROP INDEX idx_session_transcripts_session_created_at;

ALTER TABLE session_transcripts DROP COLUMN event_created_at_ns;
ALTER TABLE session_transcripts DROP COLUMN supersedes_seq;
ALTER TABLE session_transcripts DROP COLUMN segment_end_s;
ALTER TABLE session_transcripts DROP COLUMN segment_start_s;
ALTER TABLE session_transcripts DROP COLUMN window_length;
ALTER TABLE session_transcripts DROP COLUMN model;
ALTER TABLE session_transcripts DROP COLUMN type;
ALTER TABLE session_transcripts DROP COLUMN source;

-- Restore original uniqueness (NOTE: will fail if duplicates exist)
ALTER TABLE session_transcripts
    ADD CONSTRAINT session_transcripts_session_id_key UNIQUE (session_id);
