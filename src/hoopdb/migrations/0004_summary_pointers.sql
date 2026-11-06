-- 96bulls 0004: summaries become encrypted Locker objects; this table keeps only the pointer.
-- Upstream contract (relayhistory-cloud docs/pipeline-summary-short.md): the text lives only in
-- {session prefix}/summary/{short,long}.json.enc, AES-256-GCM under the workspace key. Postgres holds
-- the object key, ciphertext SHA-256, key id, status and revision, never the text or the model name
-- (Khaliq, 2026-10-29: "Supabase should ideally be pointers").
--
-- Existing rows hold plaintext summaries, which must not stay in Postgres. They are derived data:
-- Bob Costas's summary sweep (src/bob-costas/sweep.ts) re-queues every indexed session without a summary
-- row, and rebuilds each as an encrypted object from the scrubbed raw in the Locker.
DELETE FROM sessions.session_summaries;

ALTER TABLE sessions.session_summaries
  DROP COLUMN IF EXISTS summary,
  DROP COLUMN IF EXISTS model_id,
  ADD COLUMN IF NOT EXISTS status text NOT NULL
    CHECK (status IN ('ready', 'disabled', 'quarantined', 'empty', 'unsupported')),
  ADD COLUMN IF NOT EXISTS object_key text,
  ADD COLUMN IF NOT EXISTS object_sha256 text CHECK (object_sha256 ~ '^[0-9a-f]{64}$'),
  ADD COLUMN IF NOT EXISTS key_id text;

-- A pointer exists exactly when the summary is ready.
ALTER TABLE sessions.session_summaries DROP CONSTRAINT IF EXISTS session_summaries_pointer;
ALTER TABLE sessions.session_summaries ADD CONSTRAINT session_summaries_pointer CHECK (
  (status = 'ready') = (object_key IS NOT NULL AND object_sha256 IS NOT NULL AND key_id IS NOT NULL)
);
