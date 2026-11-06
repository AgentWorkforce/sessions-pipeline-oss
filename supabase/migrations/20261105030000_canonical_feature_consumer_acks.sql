-- Canonical feature consumer progress. Registry slot assigned by the unified
-- boundary generator in #242. The canonical journal and owner-only writer live
-- in 20261105025000; this migration does not copy or redefine either one.
-- Metadata-only: no payload storage or transcript bytes.

-- Companion producer-side state — REVISION-keyed page-chain ledger.
-- A retire's image_key equals its prior publish's, so progress identity is
-- the unique journal revision (kind+image bound), never image_key alone.
CREATE TABLE sessions.canonical_feature_page_ack(
  entry_revision bigint NOT NULL
    REFERENCES sessions.canonical_feature_journal(revision),
  kind text NOT NULL CHECK (kind IN ('publish','retire')),
  pool text NOT NULL CHECK (pool IN ('event','brief')),
  cursor text NOT NULL CHECK (length(cursor) BETWEEN 1 AND 4096),
  next_cursor text CHECK (next_cursor IS NULL OR
    length(next_cursor) BETWEEN 1 AND 4096), -- NULL = terminal page
  page_sha256 text NOT NULL CHECK (page_sha256 ~ '^[0-9a-f]{64}$'),
  acked_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (entry_revision, pool, cursor));
-- Page state: one dirty row per journal entry; finish CAS binds the captured
-- authority/profile and refuses on drift or a second finish.
CREATE TABLE sessions.canonical_feature_copy_state(
  entry_revision bigint PRIMARY KEY
    REFERENCES sessions.canonical_feature_journal(revision),
  kind text NOT NULL CHECK (kind IN ('publish','retire')),
  image_key text NOT NULL,
  source_authority jsonb NOT NULL, pointer_set jsonb NOT NULL,
  profile_binding text NOT NULL CHECK (length(profile_binding) BETWEEN 1 AND 512),
  target_binding text NOT NULL CHECK (length(target_binding) BETWEEN 1 AND 1024),
  dirty boolean NOT NULL DEFAULT true);
ALTER TABLE sessions.canonical_feature_page_ack ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.canonical_feature_copy_state ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.canonical_feature_page_ack,
  sessions.canonical_feature_copy_state
  FROM PUBLIC,anon,authenticated,service_role,
  bulls96_session_ingest,bulls96_session_ingest_seeder;
-- Producer progress role: SELECT on journal, write ONLY the progress tables
-- (never the journal or creator authority). The owner-defined finish RPC below
-- is the only producer-executable mutation after initialization.
GRANT SELECT ON sessions.canonical_feature_journal
  TO bulls96_canonical_feature_producer;
GRANT INSERT,SELECT ON sessions.canonical_feature_page_ack
  TO bulls96_canonical_feature_producer;
GRANT INSERT,SELECT ON sessions.canonical_feature_copy_state
  TO bulls96_canonical_feature_producer;
CREATE POLICY canonical_feature_page_ack_producer_select
  ON sessions.canonical_feature_page_ack FOR SELECT
  TO bulls96_canonical_feature_producer USING(true);
CREATE POLICY canonical_feature_page_ack_producer_insert
  ON sessions.canonical_feature_page_ack FOR INSERT
  TO bulls96_canonical_feature_producer WITH CHECK(true);
CREATE POLICY canonical_feature_copy_state_producer_select
  ON sessions.canonical_feature_copy_state FOR SELECT
  TO bulls96_canonical_feature_producer USING(true);
CREATE POLICY canonical_feature_copy_state_producer_insert
  ON sessions.canonical_feature_copy_state FOR INSERT
  TO bulls96_canonical_feature_producer WITH CHECK(dirty);

-- Every page ACK serializes on the same copy-state row as finish. A late ACK
-- can therefore never commit after finish validated the chain and flipped the
-- entry complete; wrong-kind and before-begin writes also fail closed.
CREATE FUNCTION sessions.canonical_feature_page_ack_fence()
RETURNS trigger LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=''
AS $ack_fence$
DECLARE v_dirty boolean;
BEGIN
  SELECT dirty INTO v_dirty FROM sessions.canonical_feature_copy_state
    WHERE entry_revision=NEW.entry_revision AND kind=NEW.kind FOR UPDATE;
  IF NOT FOUND OR NOT v_dirty THEN
    RAISE EXCEPTION USING ERRCODE='55000',
      MESSAGE='canonical feature copy is not dirty';
  END IF;
  RETURN NEW;
END;
$ack_fence$;
REVOKE ALL ON FUNCTION sessions.canonical_feature_page_ack_fence()
  FROM PUBLIC,anon,authenticated,service_role,
  bulls96_session_ingest,bulls96_session_ingest_seeder,
  bulls96_canonical_feature_producer;
CREATE TRIGGER canonical_feature_page_ack_fence
  BEFORE INSERT ON sessions.canonical_feature_page_ack
  FOR EACH ROW EXECUTE FUNCTION sessions.canonical_feature_page_ack_fence();

-- Server-side finish: locks the state row, matches the exact journal entry
-- and bindings, proves BOTH start-anchored acyclic page chains end at exactly
-- one terminal page, then CAS dirty->complete in one transaction.
CREATE OR REPLACE FUNCTION sessions.finish_canonical_copy(
  p_revision bigint,
  p_source_authority jsonb,
  p_profile_binding text,
  p_terminal_event text,
  p_terminal_brief text,
  p_target_binding text
) RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path='' AS $finish$
DECLARE
  v_st sessions.canonical_feature_copy_state%ROWTYPE;
  v_pool text;
  v_cursor text;
  v_next text;
  v_seen text[];
  v_steps int;
  v_count int;
  v_terminal int;
BEGIN
  SELECT * INTO v_st FROM sessions.canonical_feature_copy_state
    WHERE entry_revision=p_revision FOR UPDATE;
  IF NOT FOUND OR NOT v_st.dirty
     OR v_st.source_authority IS DISTINCT FROM p_source_authority
     OR v_st.profile_binding IS DISTINCT FROM p_profile_binding
     OR v_st.target_binding IS DISTINCT FROM p_target_binding
  THEN RETURN false; END IF;
  -- The state row must bind the exact journal row INCLUDING its authority
  -- and pointer set — a superseded or drifted entry cannot finish.
  IF NOT EXISTS(SELECT 1 FROM sessions.canonical_feature_journal j
    WHERE j.revision=p_revision AND j.image_key=v_st.image_key
      AND j.kind=v_st.kind
      AND j.source_authority=v_st.source_authority
      AND j.pointer_set=v_st.pointer_set)
  THEN RETURN false; END IF;

  -- Each pool's chain must start at 'start', be acyclic (every cursor
  -- visited once, each page's next feeds the next), and end at exactly one
  -- terminal page (next_cursor IS NULL).
  FOR v_pool IN SELECT pool FROM (VALUES('event'),('brief'))p(pool)
  LOOP
    -- A progress row with the wrong journal kind is never accepted merely
    -- because its revision and page chain happen to match.
    IF EXISTS(SELECT 1 FROM sessions.canonical_feature_page_ack
      WHERE entry_revision=p_revision AND kind<>v_st.kind)
    THEN RETURN false; END IF;
    SELECT count(*) INTO v_count FROM sessions.canonical_feature_page_ack
      WHERE entry_revision=p_revision AND kind=v_st.kind AND pool=v_pool;
    IF v_count=0 THEN RETURN false; END IF;   -- zero pages -> not finished
    -- Exactly one terminal page (next_cursor IS NULL).
    SELECT count(*) INTO v_terminal FROM sessions.canonical_feature_page_ack
      WHERE entry_revision=p_revision AND kind=v_st.kind
        AND pool=v_pool AND next_cursor IS NULL;
    IF v_terminal<>1 THEN RETURN false; END IF;
    -- Walk the chain from 'start'; a gap/branch/disconnected component or a
    -- repeated cursor (cycle) terminates before v_count steps -> refuse.
    v_cursor:='start'; v_steps:=0; v_seen:=ARRAY[]::text[];
    LOOP
      IF v_cursor=ANY(v_seen) THEN RETURN false; END IF;  -- cycle
      v_seen:=v_seen||v_cursor;
      SELECT next_cursor INTO v_next FROM sessions.canonical_feature_page_ack
        WHERE entry_revision=p_revision AND kind=v_st.kind
          AND pool=v_pool AND cursor=v_cursor;
      IF NOT FOUND THEN RETURN false; END IF;   -- dangling next -> gap
      v_steps:=v_steps+1;
      EXIT WHEN v_next IS NULL;
      v_cursor:=v_next;
    END LOOP;
    -- The chain must reach the terminal in exactly v_count steps — a branch
    -- or disconnected component leaves pages unvisited.
    IF v_steps<>v_count THEN RETURN false; END IF;
    -- The terminal page's hash must equal the supplied terminal descriptor —
    -- the produced image's own terminal, not an arbitrary page.
    IF NOT EXISTS(SELECT 1 FROM sessions.canonical_feature_page_ack
      WHERE entry_revision=p_revision AND kind=v_st.kind AND pool=v_pool
        AND next_cursor IS NULL
        AND page_sha256 = CASE WHEN v_pool='event'
          THEN p_terminal_event ELSE p_terminal_brief END)
    THEN RETURN false; END IF;
  END LOOP;
  UPDATE sessions.canonical_feature_copy_state SET dirty=false
    WHERE entry_revision=p_revision;
  RETURN true;
END;
$finish$;
REVOKE ALL ON FUNCTION sessions.finish_canonical_copy(bigint,jsonb,text,text,text,text)
  FROM PUBLIC,anon,authenticated,service_role,
  bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.finish_canonical_copy(bigint,jsonb,text,text,text,text)
  TO bulls96_canonical_feature_producer;
