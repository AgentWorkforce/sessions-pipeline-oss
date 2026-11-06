-- G6: a bounded, scrubbed short summary on the logical-session directory, and the
-- one tenant-bound search path over it.
--
-- Provenance of scope: the helm-chart-chief coordinator recorded the user's direct
-- approval to implement bounded short_summary work in its controlling session.
-- Draft only; applying or merging needs the user's OK on the exact head.
--
-- Long summaries and transcripts stay in object storage. No embeddings, pgvector,
-- tsvector/FTS, or project/model/reasoning search here: those are V2
-- (ClickHouse/ObsessionDB).
--
-- Invariants enforced here (the TypeScript writer in
-- src/hoopdb/session-directory-summary.ts enforces the same ones first):
--   - hard length bound: char_length(short_summary) <= 1000;
--   - a stored summary carries scrub provenance (ruleset, version), source and
--     generation time, all or none. The database checks those labels; only the
--     writer can actually run the scrubber, so it alone proves the text is scrubbed;
--   - a tombstoned row never holds a summary or a search document (trigger + CHECK).
--
-- Search: the summary is part of search_text. #38's metadata trigram index is not
-- tenant-scoped, so it is replaced by session_directory_tenant_search_trgm_idx
-- (org_id, workspace_id, search_text) via btree_gin, and the only search path is
-- sessions.search_session_directory: SECURITY DEFINER, fixed SQL, tenant taken from
-- the signed JWT claims, callable only by the NOLOGIN Worker role, which has no
-- table privileges and no BYPASSRLS. Direct search_text/short_summary reads by
-- `authenticated` are revoked so a PostgREST table ILIKE cannot bypass it.
--
-- Code only; nothing here is applied by this repository. The down path is
-- supabase/rollbacks/20261030020000_session_directory_short_summary.down.sql.

-- Role guard, first so that a failure leaves nothing applied. This migration never
-- creates or alters a role: creating one needs CREATEROLE, and on PostgreSQL 16+
-- ALTER ROLE ... SET needs CREATEROLE plus ADMIN on the role, which a least-privilege
-- migration owner (CNPG `hoop`, no CREATEROLE) does not have. The role is provisioned
-- beforehand by the privileged, idempotent bootstrap
-- supabase/bootstrap/session_directory_search_worker.sql (runbook:
-- runbooks/session-directory-search-worker-bootstrap.md).
--
-- The role must exist exactly as the bootstrap makes it, or the migration fails closed
-- before any DDL:
--   - absent: SQLSTATE 42704;
--   - anything else wrong: SQLSTATE 55000, naming every violation.
-- Required: NOLOGIN, NOINHERIT, NOBYPASSRLS, not SUPERUSER, CREATEROLE, CREATEDB or
-- REPLICATION. Its only setting (pg_db_role_setting) is the role-wide
-- statement_timeout=2s, spelled exactly so: '2000ms', '2000' or an extra setting are
-- refused. It is a member of no role. Allowed members are `authenticator` under the
-- existing contract and, only when the migrator binds exact `supabase` mode on PG16+,
-- the unavoidable postgres creator edge granted by superuser supabase_admin with ADMIN
-- true and INHERIT/SET false. It owns no objects and holds no explicit privileges, except USAGE on schema
-- `sessions`, which this migration grants.
DO $role_guard$
DECLARE
  v_oid oid;
  v_problems text[] := '{}';
  v_row record;
  v_settings text;
  v_mode text := coalesce(current_setting('hoopdb.db_mode', true), '');
  v_version int := current_setting('server_version_num')::int;
BEGIN
  SELECT oid INTO v_oid FROM pg_roles WHERE rolname = 'session_directory_search_worker';
  IF v_oid IS NULL THEN
    RAISE EXCEPTION USING ERRCODE = '42704',
      MESSAGE = 'role session_directory_search_worker does not exist; this migration does not create it',
      HINT = 'Run supabase/bootstrap/session_directory_search_worker.sql as a privileged role first '
             '(runbooks/session-directory-search-worker-bootstrap.md), then rerun.';
  END IF;

  SELECT * INTO v_row FROM pg_roles WHERE oid = v_oid;
  IF v_row.rolcanlogin THEN v_problems := array_append(v_problems, 'LOGIN'::text); END IF;
  IF v_row.rolinherit THEN v_problems := array_append(v_problems, 'INHERIT'::text); END IF;
  IF v_row.rolbypassrls THEN v_problems := array_append(v_problems, 'BYPASSRLS'::text); END IF;
  IF v_row.rolsuper THEN v_problems := array_append(v_problems, 'SUPERUSER'::text); END IF;
  IF v_row.rolcreaterole THEN v_problems := array_append(v_problems, 'CREATEROLE'::text); END IF;
  IF v_row.rolcreatedb THEN v_problems := array_append(v_problems, 'CREATEDB'::text); END IF;
  IF v_row.rolreplication THEN v_problems := array_append(v_problems, 'REPLICATION'::text); END IF;

  -- Settings: exactly one row, role-wide (setdatabase = 0), exactly {statement_timeout=2s}.
  IF (SELECT count(*) FROM pg_db_role_setting WHERE setrole = v_oid) <> 1
     OR NOT EXISTS (SELECT 1 FROM pg_db_role_setting
                     WHERE setrole = v_oid AND setdatabase = 0
                       AND setconfig = ARRAY['statement_timeout=2s']::text[]) THEN
    SELECT coalesce(string_agg(
             CASE WHEN s.setdatabase = 0 THEN '' ELSE format('database %s: ', s.setdatabase) END
               || array_to_string(s.setconfig, ', '), '; '), 'none')
      INTO v_settings
      FROM pg_db_role_setting AS s WHERE s.setrole = v_oid;
    v_problems := array_append(v_problems,
      format('role settings must be exactly statement_timeout=2s (found: %s)', v_settings)::text);
  END IF;

  -- Memberships: it may belong to no role; only a NOINHERIT authenticator without
  -- ADMIN OPTION may belong to it (an inheriting member would gain its EXECUTE, and an
  -- ADMIN member could grant it onward).
  FOR v_row IN SELECT r.rolname FROM pg_auth_members AS m JOIN pg_roles AS r ON r.oid = m.roleid
                WHERE m.member = v_oid LOOP
    v_problems := array_append(v_problems, format('member of %I', v_row.rolname)::text);
  END LOOP;
  IF v_version >= 160000 THEN
    FOR v_row IN EXECUTE
      'SELECT r.rolname, r.rolinherit, g.rolname AS grantor, g.rolsuper AS grantor_super,
              m.admin_option, m.inherit_option, m.set_option
         FROM pg_auth_members AS m
         JOIN pg_roles AS r ON r.oid = m.member
         JOIN pg_roles AS g ON g.oid = m.grantor
        WHERE m.roleid = $1 ORDER BY r.rolname, g.rolname' USING v_oid LOOP
      IF v_mode = 'supabase'
         AND v_row.rolname = 'postgres'
         AND v_row.grantor = 'supabase_admin' AND v_row.grantor_super
         AND v_row.admin_option AND NOT v_row.inherit_option AND NOT v_row.set_option THEN
        NULL;
      ELSIF v_row.rolname <> 'authenticator' OR v_row.rolinherit OR v_row.admin_option THEN
        v_problems := array_append(v_problems, format('granted to %I', v_row.rolname)::text);
      END IF;
    END LOOP;
  ELSE
    FOR v_row IN SELECT r.rolname, r.rolinherit, m.admin_option FROM pg_auth_members AS m
                   JOIN pg_roles AS r ON r.oid = m.member
                  WHERE m.roleid = v_oid LOOP
      IF v_row.rolname <> 'authenticator' OR v_row.rolinherit OR v_row.admin_option THEN
        v_problems := array_append(v_problems, format('granted to %I', v_row.rolname)::text);
      END IF;
    END LOOP;
  END IF;
  -- PostgreSQL 16+ records INHERIT per grant; refuse an inheriting grant even to a
  -- NOINHERIT member. (Dynamic: the column does not exist before 16.)
  IF v_version >= 160000 THEN
    FOR v_row IN EXECUTE
      'SELECT r.rolname FROM pg_auth_members AS m JOIN pg_roles AS r ON r.oid = m.member
        WHERE m.roleid = $1 AND m.inherit_option' USING v_oid LOOP
      v_problems := array_append(v_problems, format('granted WITH INHERIT to %I', v_row.rolname)::text);
    END LOOP;
  END IF;

  -- Owned objects.
  IF EXISTS (SELECT 1 FROM pg_class WHERE relowner = v_oid)
     OR EXISTS (SELECT 1 FROM pg_proc WHERE proowner = v_oid)
     OR EXISTS (SELECT 1 FROM pg_namespace WHERE nspowner = v_oid)
     OR EXISTS (SELECT 1 FROM pg_type WHERE typowner = v_oid)
     OR EXISTS (SELECT 1 FROM pg_database WHERE datdba = v_oid) THEN
    v_problems := array_append(v_problems, 'owns objects'::text);
  END IF;

  -- Explicit privileges anywhere (PUBLIC grants are not the role's own).
  IF EXISTS (SELECT 1 FROM pg_class AS c, aclexplode(c.relacl) AS a WHERE a.grantee = v_oid) THEN
    v_problems := array_append(v_problems, 'table, view or sequence privileges'::text);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_attribute AS t, aclexplode(t.attacl) AS a WHERE a.grantee = v_oid) THEN
    v_problems := array_append(v_problems, 'column privileges'::text);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_proc AS p, aclexplode(p.proacl) AS a WHERE a.grantee = v_oid) THEN
    v_problems := array_append(v_problems, 'function privileges'::text);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_namespace AS n, aclexplode(n.nspacl) AS a
              WHERE a.grantee = v_oid
                AND NOT (n.nspname = 'sessions' AND a.privilege_type = 'USAGE')) THEN
    v_problems := array_append(v_problems, 'schema privileges'::text);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_database AS d, aclexplode(d.datacl) AS a WHERE a.grantee = v_oid) THEN
    v_problems := array_append(v_problems, 'database privileges'::text);
  END IF;
  IF EXISTS (SELECT 1 FROM pg_default_acl AS d, aclexplode(d.defaclacl) AS a WHERE a.grantee = v_oid)
     OR EXISTS (SELECT 1 FROM pg_default_acl WHERE defaclrole = v_oid) THEN
    v_problems := array_append(v_problems, 'default privileges'::text);
  END IF;

  IF cardinality(v_problems) > 0 THEN
    RAISE EXCEPTION USING ERRCODE = '55000',
      MESSAGE = 'role session_directory_search_worker does not match the bootstrap spec: '
                || array_to_string(v_problems, ', '),
      HINT = 'Fix or drop the role and rerun supabase/bootstrap/session_directory_search_worker.sql '
             'as a privileged role (runbooks/session-directory-search-worker-bootstrap.md). '
             'The migration never alters a role.';
  END IF;
END
$role_guard$;

CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public;

ALTER TABLE sessions.session_directory
  ADD COLUMN short_summary text
    CONSTRAINT session_directory_short_summary_length
    CHECK (char_length(short_summary) BETWEEN 1 AND 1000),
  -- Which lib/scrub.ts rules the text went through (raw-chunks SCRUB_RULESET and
  -- SCRUB_RULESET_VERSION; the same pair the manifest records as raw.scrub).
  ADD COLUMN short_summary_scrub_ruleset text
    CONSTRAINT session_directory_short_summary_scrub_ruleset
    CHECK (short_summary_scrub_ruleset = 'relayhistory-scrub'),
  ADD COLUMN short_summary_scrub_version smallint
    CONSTRAINT session_directory_short_summary_scrub_version
    CHECK (short_summary_scrub_version >= 1),
  -- The producer and its version, e.g. 'bob-costas/extractive/v1' (the same shape as
  -- session_summaries.model_id and session_verdicts.model_id).
  ADD COLUMN short_summary_source text
    CONSTRAINT session_directory_short_summary_source
    CHECK (short_summary_source ~ '^[a-z0-9][a-z0-9._/-]{0,63}$'),
  ADD COLUMN short_summary_generated_at timestamptz;

ALTER TABLE sessions.session_directory
  ADD CONSTRAINT session_directory_short_summary_provenance CHECK (
    (short_summary IS NULL) = (short_summary_scrub_ruleset IS NULL) AND
    (short_summary IS NULL) = (short_summary_scrub_version IS NULL) AND
    (short_summary IS NULL) = (short_summary_source IS NULL) AND
    (short_summary IS NULL) = (short_summary_generated_at IS NULL)
  ),
  ADD CONSTRAINT session_directory_short_summary_not_on_tombstone CHECK (
    deleted_at IS NULL OR short_summary IS NULL
  );

COMMENT ON COLUMN sessions.session_directory.short_summary IS
  'Scrubbed short summary, at most 1000 characters. Long summaries and transcripts stay in object storage. Cleared on tombstone; removed with the row on erasure.';
COMMENT ON COLUMN sessions.session_directory.search_text IS
  'Search document built by sessions.session_directory_search_text: metadata plus the scrubbed short_summary. MUST NOT contain transcript or event content. Searched only through sessions.search_session_directory.';

-- The one definition of the search document. The summary writer uses it, and the
-- future projector must too, so short_summary is always part of search_text.
CREATE FUNCTION sessions.session_directory_search_text(
  p_title text, p_task_title text, p_agent_name text, p_project_id text,
  p_git_repo text, p_git_branch text, p_model text, p_short_summary text
) RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, pg_temp AS $search_text$
  SELECT left(concat_ws(' ',
    nullif(btrim(p_title), ''), nullif(btrim(p_task_title), ''),
    nullif(btrim(p_agent_name), ''), nullif(btrim(p_project_id), ''),
    nullif(btrim(p_git_repo), ''), nullif(btrim(p_git_branch), ''),
    nullif(btrim(p_model), ''), nullif(btrim(p_short_summary), '')), 8192)
$search_text$;

-- Deletion propagation backstop: whatever writer sets deleted_at, the row keeps no
-- summary text, in the column or in the search document that embedded it.
CREATE FUNCTION sessions.session_directory_tombstone_clears_summary()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp AS $tombstone$
BEGIN
  IF NEW.deleted_at IS NOT NULL THEN
    NEW.short_summary := NULL;
    NEW.short_summary_scrub_ruleset := NULL;
    NEW.short_summary_scrub_version := NULL;
    NEW.short_summary_source := NULL;
    NEW.short_summary_generated_at := NULL;
    NEW.search_text := '';
  END IF;
  RETURN NEW;
END
$tombstone$;

CREATE TRIGGER session_directory_tombstone_clears_summary
  BEFORE INSERT OR UPDATE ON sessions.session_directory
  FOR EACH ROW EXECUTE FUNCTION sessions.session_directory_tombstone_clears_summary();

-- Tenant-scoped search index. Replaces #38's session_directory_metadata_trgm_idx,
-- whose GIN over search_text alone mixed every tenant's text in one posting list.
DROP INDEX sessions.session_directory_metadata_trgm_idx;
CREATE INDEX session_directory_tenant_search_trgm_idx
  ON sessions.session_directory
  USING gin (org_id, workspace_id, search_text public.gin_trgm_ops)
  WHERE search_text <> '' AND deleted_at IS NULL;

-- Tenant-scoped project substring. Replaces #38's session_directory_project_trgm_idx
-- for the same reason; project_contains is served only through the RPC.
DROP INDEX sessions.session_directory_project_trgm_idx;
CREATE INDEX session_directory_tenant_project_trgm_idx
  ON sessions.session_directory
  USING gin (org_id, workspace_id, project_id public.gin_trgm_ops)
  WHERE project_id IS NOT NULL AND deleted_at IS NULL;

-- The dedicated Worker identity. NOLOGIN: the authenticator/credential binding is a
-- separately reviewed deployment step. No table privileges, no BYPASSRLS; it can
-- only execute the search RPC. PostgREST (>= v12) applies role-level settings, so
-- the role-wide statement_timeout=2s set by the bootstrap bounds every request made as
-- this role. The role guard at the top of this migration has verified it is present
-- exactly; this migration neither creates nor alters the role.
GRANT USAGE ON SCHEMA sessions TO session_directory_search_worker;

-- One normalization rule for search queries, identical to src/hoopdb/search-query.ts
-- (the character classes below are pgClass() of its pinned Unicode 17.0 ranges; a test
-- pins them). NFC; remove format characters (Cf); collapse separators and controls
-- (Z, Cc) to one space and trim; NFC again; then require at least 3 meaningful code
-- points (not that space, not a combining mark M) and at most 200 in total.
-- Returns NULL when the query is refused.
CREATE FUNCTION sessions.session_directory_search_query(p_query text)
RETURNS text LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
SET search_path = pg_catalog, pg_temp AS $query$
DECLARE
  v text;
BEGIN
  IF p_query IS NULL THEN
    RETURN NULL;
  END IF;
  v := normalize(p_query, NFC);
  v := regexp_replace(v, '[\u00AD\u0600-\u0605\u061C\u06DD\u070F\u0890-\u0891\u08E2\u180E\u200B-\u200F\u202A-\u202E\u2060-\u2064\u2066-\u206F\uFEFF\uFFF9-\uFFFB\U000110BD\U000110CD\U00013430-\U0001343F\U0001BCA0-\U0001BCA3\U0001D173-\U0001D17A\U000E0001\U000E0020-\U000E007F]', '', 'g');
  v := regexp_replace(v, '[\u0001-\u0020\u007F-\u00A0\u1680\u2000-\u200A\u2028-\u2029\u202F\u205F\u3000]+', ' ', 'g');
  v := btrim(v, ' ');
  v := normalize(v, NFC);
  IF char_length(regexp_replace(v, '[ \u0300-\u036F\u0483-\u0489\u0591-\u05BD\u05BF\u05C1-\u05C2\u05C4-\u05C5\u05C7\u0610-\u061A\u064B-\u065F\u0670\u06D6-\u06DC\u06DF-\u06E4\u06E7-\u06E8\u06EA-\u06ED\u0711\u0730-\u074A\u07A6-\u07B0\u07EB-\u07F3\u07FD\u0816-\u0819\u081B-\u0823\u0825-\u0827\u0829-\u082D\u0859-\u085B\u0897-\u089F\u08CA-\u08E1\u08E3-\u0903\u093A-\u093C\u093E-\u094F\u0951-\u0957\u0962-\u0963\u0981-\u0983\u09BC\u09BE-\u09C4\u09C7-\u09C8\u09CB-\u09CD\u09D7\u09E2-\u09E3\u09FE\u0A01-\u0A03\u0A3C\u0A3E-\u0A42\u0A47-\u0A48\u0A4B-\u0A4D\u0A51\u0A70-\u0A71\u0A75\u0A81-\u0A83\u0ABC\u0ABE-\u0AC5\u0AC7-\u0AC9\u0ACB-\u0ACD\u0AE2-\u0AE3\u0AFA-\u0AFF\u0B01-\u0B03\u0B3C\u0B3E-\u0B44\u0B47-\u0B48\u0B4B-\u0B4D\u0B55-\u0B57\u0B62-\u0B63\u0B82\u0BBE-\u0BC2\u0BC6-\u0BC8\u0BCA-\u0BCD\u0BD7\u0C00-\u0C04\u0C3C\u0C3E-\u0C44\u0C46-\u0C48\u0C4A-\u0C4D\u0C55-\u0C56\u0C62-\u0C63\u0C81-\u0C83\u0CBC\u0CBE-\u0CC4\u0CC6-\u0CC8\u0CCA-\u0CCD\u0CD5-\u0CD6\u0CE2-\u0CE3\u0CF3\u0D00-\u0D03\u0D3B-\u0D3C\u0D3E-\u0D44\u0D46-\u0D48\u0D4A-\u0D4D\u0D57\u0D62-\u0D63\u0D81-\u0D83\u0DCA\u0DCF-\u0DD4\u0DD6\u0DD8-\u0DDF\u0DF2-\u0DF3\u0E31\u0E34-\u0E3A\u0E47-\u0E4E\u0EB1\u0EB4-\u0EBC\u0EC8-\u0ECE\u0F18-\u0F19\u0F35\u0F37\u0F39\u0F3E-\u0F3F\u0F71-\u0F84\u0F86-\u0F87\u0F8D-\u0F97\u0F99-\u0FBC\u0FC6\u102B-\u103E\u1056-\u1059\u105E-\u1060\u1062-\u1064\u1067-\u106D\u1071-\u1074\u1082-\u108D\u108F\u109A-\u109D\u135D-\u135F\u1712-\u1715\u1732-\u1734\u1752-\u1753\u1772-\u1773\u17B4-\u17D3\u17DD\u180B-\u180D\u180F\u1885-\u1886\u18A9\u1920-\u192B\u1930-\u193B\u1A17-\u1A1B\u1A55-\u1A5E\u1A60-\u1A7C\u1A7F\u1AB0-\u1ADD\u1AE0-\u1AEB\u1B00-\u1B04\u1B34-\u1B44\u1B6B-\u1B73\u1B80-\u1B82\u1BA1-\u1BAD\u1BE6-\u1BF3\u1C24-\u1C37\u1CD0-\u1CD2\u1CD4-\u1CE8\u1CED\u1CF4\u1CF7-\u1CF9\u1DC0-\u1DFF\u20D0-\u20F0\u2CEF-\u2CF1\u2D7F\u2DE0-\u2DFF\u302A-\u302F\u3099-\u309A\uA66F-\uA672\uA674-\uA67D\uA69E-\uA69F\uA6F0-\uA6F1\uA802\uA806\uA80B\uA823-\uA827\uA82C\uA880-\uA881\uA8B4-\uA8C5\uA8E0-\uA8F1\uA8FF\uA926-\uA92D\uA947-\uA953\uA980-\uA983\uA9B3-\uA9C0\uA9E5\uAA29-\uAA36\uAA43\uAA4C-\uAA4D\uAA7B-\uAA7D\uAAB0\uAAB2-\uAAB4\uAAB7-\uAAB8\uAABE-\uAABF\uAAC1\uAAEB-\uAAEF\uAAF5-\uAAF6\uABE3-\uABEA\uABEC-\uABED\uFB1E\uFE00-\uFE0F\uFE20-\uFE2F\U000101FD\U000102E0\U00010376-\U0001037A\U00010A01-\U00010A03\U00010A05-\U00010A06\U00010A0C-\U00010A0F\U00010A38-\U00010A3A\U00010A3F\U00010AE5-\U00010AE6\U00010D24-\U00010D27\U00010D69-\U00010D6D\U00010EAB-\U00010EAC\U00010EFA-\U00010EFF\U00010F46-\U00010F50\U00010F82-\U00010F85\U00011000-\U00011002\U00011038-\U00011046\U00011070\U00011073-\U00011074\U0001107F-\U00011082\U000110B0-\U000110BA\U000110C2\U00011100-\U00011102\U00011127-\U00011134\U00011145-\U00011146\U00011173\U00011180-\U00011182\U000111B3-\U000111C0\U000111C9-\U000111CC\U000111CE-\U000111CF\U0001122C-\U00011237\U0001123E\U00011241\U000112DF-\U000112EA\U00011300-\U00011303\U0001133B-\U0001133C\U0001133E-\U00011344\U00011347-\U00011348\U0001134B-\U0001134D\U00011357\U00011362-\U00011363\U00011366-\U0001136C\U00011370-\U00011374\U000113B8-\U000113C0\U000113C2\U000113C5\U000113C7-\U000113CA\U000113CC-\U000113D0\U000113D2\U000113E1-\U000113E2\U00011435-\U00011446\U0001145E\U000114B0-\U000114C3\U000115AF-\U000115B5\U000115B8-\U000115C0\U000115DC-\U000115DD\U00011630-\U00011640\U000116AB-\U000116B7\U0001171D-\U0001172B\U0001182C-\U0001183A\U00011930-\U00011935\U00011937-\U00011938\U0001193B-\U0001193E\U00011940\U00011942-\U00011943\U000119D1-\U000119D7\U000119DA-\U000119E0\U000119E4\U00011A01-\U00011A0A\U00011A33-\U00011A39\U00011A3B-\U00011A3E\U00011A47\U00011A51-\U00011A5B\U00011A8A-\U00011A99\U00011B60-\U00011B67\U00011C2F-\U00011C36\U00011C38-\U00011C3F\U00011C92-\U00011CA7\U00011CA9-\U00011CB6\U00011D31-\U00011D36\U00011D3A\U00011D3C-\U00011D3D\U00011D3F-\U00011D45\U00011D47\U00011D8A-\U00011D8E\U00011D90-\U00011D91\U00011D93-\U00011D97\U00011EF3-\U00011EF6\U00011F00-\U00011F01\U00011F03\U00011F34-\U00011F3A\U00011F3E-\U00011F42\U00011F5A\U00013440\U00013447-\U00013455\U0001611E-\U0001612F\U00016AF0-\U00016AF4\U00016B30-\U00016B36\U00016F4F\U00016F51-\U00016F87\U00016F8F-\U00016F92\U00016FE4\U00016FF0-\U00016FF1\U0001BC9D-\U0001BC9E\U0001CF00-\U0001CF2D\U0001CF30-\U0001CF46\U0001D165-\U0001D169\U0001D16D-\U0001D172\U0001D17B-\U0001D182\U0001D185-\U0001D18B\U0001D1AA-\U0001D1AD\U0001D242-\U0001D244\U0001DA00-\U0001DA36\U0001DA3B-\U0001DA6C\U0001DA75\U0001DA84\U0001DA9B-\U0001DA9F\U0001DAA1-\U0001DAAF\U0001E000-\U0001E006\U0001E008-\U0001E018\U0001E01B-\U0001E021\U0001E023-\U0001E024\U0001E026-\U0001E02A\U0001E08F\U0001E130-\U0001E136\U0001E2AE\U0001E2EC-\U0001E2EF\U0001E4EC-\U0001E4EF\U0001E5EE-\U0001E5EF\U0001E6E3\U0001E6E6\U0001E6EE-\U0001E6EF\U0001E6F5\U0001E8D0-\U0001E8D6\U0001E944-\U0001E94A\U000E0100-\U000E01EF]', '', 'g')) < 3
     OR char_length(v) > 200 THEN
    RETURN NULL;
  END IF;
  RETURN v;
END
$query$;

-- The only search path for search_text and short_summary, and the tenant-bound list
-- path for every filter the RelayHistory read surface exposes (contract:
-- docs/session-search-index-contract.md, "The search RPC (pinned contract)").
--
-- SQL text is assembled only from the fixed fragments below, chosen by which
-- parameters are set; every value is bound with EXECUTE ... USING, never
-- interpolated. Fixed ORDER BY (recency keyset), at most p_limit + 1 <= 51 rows. The
-- organization and the workspaces come only from the signed claims. Errors:
--   42501  caller is not the Worker role; or claims missing, malformed, for another
--          role, or not covering the workspace
--   22023  an invalid value: text needing 3 to 200 meaningful characters after
--          sessions.session_directory_search_query normalization (p_query,
--          p_project_contains, prefix p_agent), limit outside 1..50, an unknown
--          state, agent match or harness format, an incomplete or mismatched cursor,
--          since after until, or a contradictory filter pair
--   0A000  an intentionally unsupported combination (named in the message)
--   54000  agent_prefix_too_broad: more than 5000 candidates for an agent prefix
-- The one issuer and audience the RPC accepts. PostgREST verifies the signature, but
-- a project can trust several keys and may not check `aud`, so the RPC pins both.
-- No row is seeded: search is refused until an environment adds its own, and adding
-- it is a deployment step. Only the owner reads this table, through the RPC.
CREATE TABLE sessions.session_directory_search_token_policy (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  issuer text NOT NULL CHECK (issuer ~ '^https?://'),
  audience text NOT NULL CHECK (audience <> ''),
  max_lifetime_seconds integer NOT NULL DEFAULT 60
    CHECK (max_lifetime_seconds BETWEEN 1 AND 60)
);
ALTER TABLE sessions.session_directory_search_token_policy ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.session_directory_search_token_policy
  FROM PUBLIC, anon, authenticated, service_role;

CREATE FUNCTION sessions.search_session_directory(
  p_workspace_id text DEFAULT NULL,
  p_query text DEFAULT NULL,
  p_harness text DEFAULT NULL,
  p_project_id text DEFAULT NULL,
  p_project_contains text DEFAULT NULL,
  p_no_project boolean DEFAULT false,
  p_agent text DEFAULT NULL,
  p_agent_match text DEFAULT 'exact',
  p_state text DEFAULT NULL,
  p_user_id text DEFAULT NULL,
  p_machine_id text DEFAULT NULL,
  p_since timestamptz DEFAULT NULL,
  p_until timestamptz DEFAULT NULL,
  p_session_id text DEFAULT NULL,
  p_cursor_last_activity_at timestamptz DEFAULT NULL,
  p_cursor_session_id text DEFAULT NULL,
  p_cursor_harness text DEFAULT NULL,
  p_cursor_workspace_id text DEFAULT NULL,
  p_limit integer DEFAULT 50
)
RETURNS TABLE (
  org_id text, workspace_id text, harness text, session_id text,
  created_at timestamptz, last_activity_at timestamptz,
  title text, task_title text, agent_name text, project_id text,
  git_repo text, git_branch text, model text, state text,
  event_count bigint, input_tokens bigint, output_tokens bigint, cost_usd_micros bigint,
  machine_ids text[], user_ids text[], short_summary text, projected_at timestamptz
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, sessions, pg_temp AS $search$
DECLARE
  v_claims jsonb;
  v_policy sessions.session_directory_search_token_policy%ROWTYPE;
  v_org text;
  v_workspace_claim jsonb;
  v_workspaces text[];
  v_query text;
  v_project_contains text;
  v_agent text;
  v_has_cursor boolean;
  v_membership text;          -- NULL, 'users' or 'machines': the driving membership table
  v_from text;
  v_where text;
  v_order text;
  v_sql text;
  v_candidates bigint;
  v_enable_seqscan text;
  v_enable_bitmapscan text;
  v_enable_sort text;
BEGIN
  -- Fail closed on the caller identity itself, whatever EXECUTE grants a later
  -- runner adds (e.g. GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sessions TO <app>):
  -- inside SECURITY DEFINER the `role` setting still names the caller's SET ROLE.
  IF coalesce(nullif(current_setting('role', true), 'none'), session_user::text)
       IS DISTINCT FROM 'session_directory_search_worker' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session search: caller is not the search worker role';
  END IF;
  BEGIN
    v_claims := nullif(current_setting('request.jwt.claims', true), '')::jsonb;
  EXCEPTION WHEN others THEN
    v_claims := NULL;
  END;
  IF v_claims IS NULL OR jsonb_typeof(v_claims) <> 'object'
     OR (v_claims ->> 'role') IS DISTINCT FROM 'session_directory_search_worker'
     OR jsonb_typeof(v_claims -> 'app_metadata') IS DISTINCT FROM 'object'
     OR jsonb_typeof(v_claims -> 'app_metadata' -> 'org_id') IS DISTINCT FROM 'string'
     OR jsonb_typeof(v_claims -> 'app_metadata' -> 'workspace_ids') IS DISTINCT FROM 'array' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session search: tenant claims missing or invalid';
  END IF;
  SELECT * INTO v_policy
    FROM sessions.session_directory_search_token_policy WHERE singleton;
  IF NOT FOUND
     OR (v_claims ->> 'iss') IS DISTINCT FROM v_policy.issuer
     OR jsonb_typeof(v_claims -> 'aud') IS DISTINCT FROM 'string'
     OR (v_claims ->> 'aud') IS DISTINCT FROM v_policy.audience
     OR jsonb_typeof(v_claims -> 'exp') IS DISTINCT FROM 'number'
     OR jsonb_typeof(v_claims -> 'iat') IS DISTINCT FROM 'number'
     OR (v_claims ->> 'exp')::numeric - (v_claims ->> 'iat')::numeric
          NOT BETWEEN 1 AND v_policy.max_lifetime_seconds
     -- A far-future iat must not stretch the window; 30s allows for clock skew.
     OR (v_claims ->> 'exp')::numeric - extract(epoch FROM clock_timestamp())
          > v_policy.max_lifetime_seconds + 30 THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session search: tenant claims missing or invalid';
  END IF;
  v_org := v_claims -> 'app_metadata' ->> 'org_id';
  -- 1 to 64 unique ids of 1 to 128 non-space characters. One bad entry refuses the
  -- whole claim, because an issuer that emits one cannot be trusted for the others.
  -- Size is checked first so an oversized claim is never expanded.
  v_workspace_claim := v_claims -> 'app_metadata' -> 'workspace_ids';
  IF jsonb_array_length(v_workspace_claim) NOT BETWEEN 1 AND 64 THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session search: tenant claims missing or invalid';
  END IF;
  SELECT coalesce(array_agg(DISTINCT w.value #>> '{}'), '{}') INTO v_workspaces
    FROM jsonb_array_elements(v_workspace_claim) AS w
   WHERE jsonb_typeof(w.value) = 'string'
     AND (w.value #>> '{}') ~ '^[^[:space:][:cntrl:]]{1,128}$';
  -- p_workspace_id NULL means organization-wide over exactly the claimed workspaces.
  IF v_org = '' OR cardinality(v_workspaces) <> jsonb_array_length(v_workspace_claim)
     OR (p_workspace_id IS NOT NULL AND NOT (p_workspace_id = ANY (v_workspaces))) THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'session search: tenant claims missing or invalid';
  END IF;

  -- Values.
  IF p_limit IS NULL OR p_limit < 1 OR p_limit > 50 THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session search: limit must be 1 to 50';
  END IF;
  IF p_query IS NOT NULL THEN
    v_query := sessions.session_directory_search_query(p_query);
    IF v_query IS NULL THEN
      RAISE EXCEPTION USING ERRCODE = '22023',
        MESSAGE = 'session search: query needs 3 to 200 meaningful characters';
    END IF;
  END IF;
  IF p_project_contains IS NOT NULL THEN
    v_project_contains := sessions.session_directory_search_query(p_project_contains);
    IF v_project_contains IS NULL THEN
      RAISE EXCEPTION USING ERRCODE = '22023',
        MESSAGE = 'session search: project_contains needs 3 to 200 meaningful characters';
    END IF;
  END IF;
  IF p_agent_match IS NULL OR p_agent_match NOT IN ('exact', 'prefix') THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session search: agent_match must be exact or prefix';
  END IF;
  IF p_agent IS NOT NULL THEN
    v_agent := lower(btrim(p_agent));
    IF v_agent = '' OR char_length(v_agent) > 200 THEN
      RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session search: agent must be 1 to 200 characters';
    END IF;
    IF p_agent_match = 'prefix' AND sessions.session_directory_search_query(p_agent) IS NULL THEN
      RAISE EXCEPTION USING ERRCODE = '22023',
        MESSAGE = 'session search: agent prefix needs at least 3 meaningful characters';
    END IF;
  END IF;
  IF p_state IS NOT NULL AND p_state NOT IN ('active', 'done', 'abandoned', 'unknown') THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session search: state is invalid';
  END IF;
  IF p_harness IS NOT NULL AND p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$' THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session search: harness is invalid';
  END IF;
  IF p_since IS NOT NULL AND p_until IS NOT NULL AND p_since > p_until THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session search: since is after until';
  END IF;
  IF coalesce(p_no_project, false) AND (p_project_id IS NOT NULL OR p_project_contains IS NOT NULL) THEN
    RAISE EXCEPTION USING ERRCODE = '22023',
      MESSAGE = 'session search: no_project contradicts project_id or project_contains';
  END IF;
  v_has_cursor := p_cursor_last_activity_at IS NOT NULL OR p_cursor_session_id IS NOT NULL
    OR p_cursor_harness IS NOT NULL OR p_cursor_workspace_id IS NOT NULL;
  IF v_has_cursor AND (p_cursor_last_activity_at IS NULL OR p_cursor_session_id IS NULL OR p_cursor_harness IS NULL
       OR (p_workspace_id IS NULL AND p_cursor_workspace_id IS NULL)
       OR (p_workspace_id IS NOT NULL AND p_cursor_workspace_id IS NOT NULL AND p_cursor_workspace_id <> p_workspace_id)) THEN
    RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'session search: cursor is incomplete or for another scope';
  END IF;

  -- Intentionally unsupported combinations.
  IF p_workspace_id IS NULL AND (p_agent IS NOT NULL OR p_project_id IS NOT NULL OR p_project_contains IS NOT NULL
       OR coalesce(p_no_project, false) OR p_user_id IS NOT NULL OR p_machine_id IS NOT NULL) THEN
    RAISE EXCEPTION USING ERRCODE = '0A000',
      MESSAGE = 'session search: unsupported combination: agent, project, no_project, user and machine filters need p_workspace_id';
  END IF;

  -- Agent prefix candidate budget: count the prefix's candidates in the tenant-scoped
  -- agent index, stopping after 5001, before running the page. More than 5000 (the page of
  -- 50 x the 100x row-amplification gate) is refused as agent_prefix_too_broad (54000)
  -- instead of reading the whole tenant. A longer prefix or an exact agent is needed.
  IF v_agent IS NOT NULL AND p_agent_match = 'prefix' THEN
    -- The probe must be an ordered walk of the agent index whatever the planner's
    -- estimate, so the bound holds by construction: no table scan, no bitmap that
    -- materializes every match, no sort. Restored before the page query.
    v_enable_seqscan := current_setting('enable_seqscan');
    v_enable_bitmapscan := current_setting('enable_bitmapscan');
    v_enable_sort := current_setting('enable_sort');
    PERFORM set_config('enable_seqscan', 'off', true), set_config('enable_bitmapscan', 'off', true),
            set_config('enable_sort', 'off', true);
    EXECUTE 'SELECT count(*) FROM (SELECT 1 FROM sessions.session_directory AS d'
      || ' WHERE d.org_id = $1 AND d.workspace_id = $2 AND d.deleted_at IS NULL'
      || ' AND d.agent_name_normalized IS NOT NULL'
      || ' AND d.agent_name_normalized LIKE $3 ESCAPE ''\'''
      -- text_pattern_ops order: only the tenant-scoped agent index supplies it, so the
      -- probe walks that index and stops after 5001 entries (never a table scan).
      || ' ORDER BY d.agent_name_normalized USING ~<~ LIMIT 5001) AS c'
      INTO v_candidates
      USING v_org, p_workspace_id,
            replace(replace(replace(v_agent, '\', '\\'), '%', '\%'), '_', '\_') || '%';
    PERFORM set_config('enable_seqscan', v_enable_seqscan, true),
            set_config('enable_bitmapscan', v_enable_bitmapscan, true),
            set_config('enable_sort', v_enable_sort, true);
    IF v_candidates > 5000 THEN
      RAISE EXCEPTION USING ERRCODE = '54000',
        MESSAGE = 'session search: agent_prefix_too_broad: more than 5000 sessions match this agent prefix',
        HINT = 'Use a longer agent prefix, or p_agent_match => ''exact''.';
    END IF;
  END IF;

  -- Statement. Membership filters drive from their ordered membership index, whose
  -- last_activity_at the projector keeps equal to the directory row's.
  IF p_user_id IS NOT NULL THEN
    v_membership := 'users';
  ELSIF p_machine_id IS NOT NULL THEN
    v_membership := 'machines';
  END IF;
  IF v_membership IS NULL THEN
    v_from := ' FROM sessions.session_directory AS d';
    v_order := ' ORDER BY d.last_activity_at DESC, d.session_id DESC, d.harness DESC'
      || CASE WHEN p_workspace_id IS NULL THEN ', d.workspace_id DESC' ELSE '' END;
  ELSE
    v_from := format(' FROM sessions.session_directory_%s AS m JOIN sessions.session_directory AS d'
      || ' ON d.org_id = m.org_id AND d.workspace_id = m.workspace_id'
      || ' AND d.harness = m.harness AND d.session_id = m.session_id', v_membership);
    v_order := ' ORDER BY m.last_activity_at DESC, m.session_id DESC, m.harness DESC';
  END IF;
  v_where := ' WHERE d.org_id = $1 AND d.deleted_at IS NULL'
    || CASE WHEN p_workspace_id IS NULL THEN ' AND d.workspace_id = ANY ($3)' ELSE ' AND d.workspace_id = $2' END
    || CASE WHEN v_membership IS NULL THEN '' ELSE ' AND m.org_id = $1 AND m.workspace_id = $2' END
    || CASE v_membership WHEN 'users' THEN ' AND m.user_id = $13' WHEN 'machines' THEN ' AND m.machine_id = $14' ELSE '' END
    || CASE WHEN v_membership = 'users' AND p_machine_id IS NOT NULL THEN
         ' AND EXISTS (SELECT 1 FROM sessions.session_directory_machines AS mm WHERE mm.org_id = d.org_id'
         || ' AND mm.workspace_id = d.workspace_id AND mm.harness = d.harness AND mm.session_id = d.session_id'
         || ' AND mm.machine_id = $14)' ELSE '' END
    || CASE WHEN v_query IS NULL THEN '' ELSE ' AND d.search_text <> '''' AND d.search_text ILIKE $4 ESCAPE ''\''' END
    || CASE WHEN p_harness IS NULL THEN '' ELSE ' AND d.harness = $5' END
    || CASE WHEN p_project_id IS NULL THEN '' ELSE ' AND d.project_id = $6' END
    || CASE WHEN v_project_contains IS NULL THEN '' ELSE ' AND d.project_id ILIKE $7 ESCAPE ''\''' END
    || CASE WHEN coalesce(p_no_project, false) THEN ' AND d.project_id IS NULL' ELSE '' END
    || CASE WHEN v_agent IS NULL THEN ''
            WHEN p_agent_match = 'exact' THEN ' AND d.agent_name_normalized = $8'
            ELSE ' AND d.agent_name_normalized LIKE $8 ESCAPE ''\''' END
    || CASE WHEN p_state IS NULL THEN '' ELSE ' AND d.state = $9' END
    || CASE WHEN p_since IS NULL THEN '' ELSE ' AND d.last_activity_at >= $10' END
    || CASE WHEN p_until IS NULL THEN '' ELSE ' AND d.last_activity_at <= $11' END
    || CASE WHEN p_session_id IS NULL THEN '' ELSE ' AND d.session_id = $12' END
    || CASE WHEN NOT v_has_cursor THEN ''
            WHEN v_membership IS NOT NULL THEN ' AND (m.last_activity_at, m.session_id, m.harness) < ($15, $16, $17)'
            WHEN p_workspace_id IS NULL THEN
              ' AND (d.last_activity_at, d.session_id, d.harness, d.workspace_id) < ($15, $16, $17, $18)'
            ELSE ' AND (d.last_activity_at, d.session_id, d.harness) < ($15, $16, $17)' END;
  v_sql := 'SELECT d.org_id, d.workspace_id, d.harness, d.session_id, d.created_at, d.last_activity_at,'
    || ' d.title, d.task_title, d.agent_name, d.project_id, d.git_repo, d.git_branch, d.model, d.state,'
    || ' d.event_count, d.input_tokens, d.output_tokens, d.cost_usd_micros,'
    || ' d.machine_ids, d.user_ids, d.short_summary, d.projected_at'
    || v_from || v_where || v_order || ' LIMIT $19';

  RETURN QUERY EXECUTE v_sql
  USING v_org, p_workspace_id, v_workspaces,
        CASE WHEN v_query IS NULL THEN NULL
             ELSE '%' || replace(replace(replace(v_query, '\', '\\'), '%', '\%'), '_', '\_') || '%' END,
        p_harness, p_project_id,
        CASE WHEN v_project_contains IS NULL THEN NULL
             ELSE '%' || replace(replace(replace(v_project_contains, '\', '\\'), '%', '\%'), '_', '\_') || '%' END,
        CASE WHEN v_agent IS NULL THEN NULL
             WHEN p_agent_match = 'exact' THEN v_agent
             ELSE replace(replace(replace(v_agent, '\', '\\'), '%', '\%'), '_', '\_') || '%' END,
        p_state, p_since, p_until, p_session_id, p_user_id, p_machine_id,
        p_cursor_last_activity_at, p_cursor_session_id, p_cursor_harness, p_cursor_workspace_id,
        p_limit + 1;
END
$search$;

REVOKE ALL ON FUNCTION sessions.search_session_directory(text, text, text, text, text, boolean, text, text, text, text, text, timestamptz, timestamptz, text, timestamptz, text, text, text, integer)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION sessions.search_session_directory(text, text, text, text, text, boolean, text, text, text, text, text, timestamptz, timestamptz, text, timestamptz, text, text, text, integer)
  TO session_directory_search_worker;
-- Remove every other grantee present at creation (ALTER DEFAULT PRIVILEGES, a
-- pre-existing runtime role such as relayhistory_app), whatever the migration order.
-- A grant issued after this migration is refused by the role guard in the function.
DO $revoke$
DECLARE
  v_grantee text;
BEGIN
  FOR v_grantee IN
    SELECT r.rolname
      FROM pg_proc AS p
      CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS a
      JOIN pg_roles AS r ON r.oid = a.grantee
     WHERE p.oid = 'sessions.search_session_directory(text, text, text, text, text, boolean, text, text, text, text, text, timestamptz, timestamptz, text, timestamptz, text, text, text, integer)'::regprocedure
       AND a.privilege_type = 'EXECUTE'
       AND a.grantee <> p.proowner
       AND r.rolname <> 'session_directory_search_worker'
  LOOP
    EXECUTE format('REVOKE ALL ON FUNCTION sessions.search_session_directory(text, text, text, text, text, boolean, text, text, text, text, text, timestamptz, timestamptz, text, timestamptz, text, text, text, integer) FROM %I', v_grantee);
  END LOOP;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'relayhistory_app') THEN
    REVOKE ALL ON FUNCTION sessions.search_session_directory(text, text, text, text, text, boolean, text, text, text, text, text, timestamptz, timestamptz, text, timestamptz, text, text, text, integer) FROM relayhistory_app;
  END IF;
END
$revoke$;

-- No direct search by end-user tokens: `authenticated` keeps RLS-scoped reads of the
-- listing columns but not search_text or short_summary, so a PostgREST
-- `search_text=ilike.*q*` table filter is refused instead of sequentially scanning.
REVOKE SELECT ON sessions.session_directory FROM authenticated;
GRANT SELECT (
  org_id, workspace_id, harness, session_id, created_at, last_activity_at,
  title, task_title, agent_name, agent_name_normalized, project_id, git_repo, git_branch,
  model, state, event_count, input_tokens, output_tokens, cost_usd_micros,
  machine_ids, user_ids, projection_version, projected_at, deleted_at
) ON sessions.session_directory TO authenticated;

REVOKE ALL ON FUNCTION sessions.session_directory_search_text(
  text, text, text, text, text, text, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.session_directory_tombstone_clears_summary() FROM PUBLIC;
REVOKE ALL ON FUNCTION sessions.session_directory_search_query(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION sessions.session_directory_search_text(
  text, text, text, text, text, text, text, text) TO service_role;
