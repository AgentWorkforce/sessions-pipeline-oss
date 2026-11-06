-- Append-only release rename (registry v2); upstream provenance: 20261103050000_session_desktop_catalog.sql. Never applied under the upstream filename in this release.
-- Fail fast instead of queueing live readers/writers behind a blocked DDL lock (migrate.ts applies
-- each file in one transaction, so SET LOCAL lasts exactly as long as this file).
SET LOCAL lock_timeout='5s';
-- Metadata only: full transcript/analysis bytes remain in canonical object storage.
-- Existing rows default unready; controlled complete-source reprojection is required.
-- Constant defaults: no table rewrite. The CHECKs are added NOT VALID (enforced for every new or
-- updated row; existing rows hold the default/NULL and satisfy them), so the ACCESS EXCLUSIVE lock is
-- not held for a validation scan. An operator runs VALIDATE CONSTRAINT later (SHARE UPDATE EXCLUSIVE,
-- readers and writers keep running); see runbooks/history-620-operations.md.
ALTER TABLE sessions.session_directory
 ADD COLUMN catalog_read_version smallint NOT NULL DEFAULT 0,
 ADD COLUMN catalog_read_epoch bigint,
 ADD COLUMN catalog_read_projected_at timestamptz,
 ADD COLUMN catalog_models_complete boolean NOT NULL DEFAULT false,
 ADD COLUMN catalog_relationships_complete boolean NOT NULL DEFAULT false,
 ADD CONSTRAINT session_directory_catalog_read_version_check CHECK(catalog_read_version IN(0,1))NOT VALID;
ALTER TABLE sessions.session_directory_relationships
 ADD COLUMN child_model text,
 ADD COLUMN child_agent_name text,
 ADD COLUMN child_agent_type text,
 ADD CONSTRAINT session_directory_relationships_child_model_check CHECK(child_model IS NULL OR char_length(child_model)<=200)NOT VALID,
 ADD CONSTRAINT session_directory_relationships_child_agent_name_check CHECK(child_agent_name IS NULL OR char_length(child_agent_name)<=200)NOT VALID,
 ADD CONSTRAINT session_directory_relationships_child_agent_type_check CHECK(child_agent_type IS NULL OR char_length(child_agent_type)<=200)NOT VALID;
CREATE INDEX session_directory_catalog_read_pending_idx ON sessions.session_directory(org_id,workspace_id)
 WHERE deleted_at IS NULL AND (catalog_read_version<>1 OR catalog_read_epoch IS DISTINCT FROM event_projection_epoch OR catalog_read_projected_at IS DISTINCT FROM catalog_projected_at OR NOT catalog_relationships_complete);

CREATE FUNCTION sessions.session_directory_desktop_catalog(p_workspace_id text,p_harness text,p_session_id text)
RETURNS SETOF jsonb LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path=pg_catalog,sessions,pg_temp AS $catalog$
DECLARE
 v_org text; d sessions.session_directory%ROWTYPE; v_models text[];
 v_parents jsonb;v_children jsonb;v_body jsonb;v_truncated boolean;
BEGIN
 v_org:=sessions.session_directory_events_require_ready(p_workspace_id);
 IF p_harness IS NULL OR p_harness !~ '^[a-z0-9][a-z0-9._-]{0,63}$'
 OR p_session_id IS NULL OR char_length(p_session_id) NOT BETWEEN 1 AND 255 THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='desktop catalog: invalid identity';
 END IF;
 -- An unprojected parent's edge can be absent entirely. A scoped lookup alone
 -- cannot prove a child has no parent, so require the whole metadata edge index.
 IF EXISTS(SELECT 1 FROM sessions.session_directory WHERE org_id=v_org AND workspace_id=p_workspace_id AND deleted_at IS NULL
   AND (catalog_read_version<>1 OR catalog_read_epoch IS DISTINCT FROM event_projection_epoch
        OR catalog_read_projected_at IS DISTINCT FROM catalog_projected_at OR NOT catalog_relationships_complete)) THEN
  RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='desktop catalog: relationship_metadata_not_ready';
 END IF;
 SELECT * INTO d FROM sessions.session_directory WHERE org_id=v_org AND workspace_id=p_workspace_id
  AND harness=p_harness AND session_id=p_session_id AND deleted_at IS NULL;
 IF NOT FOUND THEN
  v_body:=jsonb_build_object('version',1,'exists',false,'identity',jsonb_build_object('orgId',v_org,'workspaceId',p_workspace_id,'source',p_harness,'sessionId',p_session_id));
  RETURN NEXT v_body||jsonb_build_object('fingerprint',encode(sha256(convert_to(v_body::text,'UTF8')),'hex'));RETURN;
 END IF;
 IF NOT d.catalog_models_complete THEN
  RAISE EXCEPTION USING ERRCODE='0A000',MESSAGE='desktop catalog: model_metadata_not_complete';
 END IF;
 SELECT coalesce(array_agg(model ORDER BY model COLLATE "C"),'{}') INTO v_models
 FROM (SELECT DISTINCT value AS model FROM unnest(d.catalog_models||d.event_models) value WHERE value IS NOT NULL AND value<>'') all_models;
 SELECT coalesce(jsonb_agg(edge ORDER BY parent_id,uid),'[]'::jsonb),count(*)>500 INTO v_parents,v_truncated
 FROM (SELECT r.parent_session_id parent_id,r.relationship_uid uid,jsonb_build_object(
   'relationshipUid',r.relationship_uid,'parentSessionId',r.parent_session_id,'childSessionId',r.child_session_id,
   'childModel',r.child_model,'childAgentName',r.child_agent_name,'childAgentType',r.child_agent_type) edge
  FROM sessions.session_directory_relationships r WHERE r.org_id=v_org AND r.workspace_id=p_workspace_id
    AND r.harness=p_harness AND r.child_session_id=p_session_id ORDER BY r.parent_session_id,r.relationship_uid LIMIT 501) parents;
 IF jsonb_array_length(v_parents)>500 THEN v_parents:=v_parents-500; END IF;
 SELECT coalesce(jsonb_agg(edge ORDER BY uid),'[]'::jsonb),v_truncated OR count(*)>500 INTO v_children,v_truncated
 FROM (SELECT r.relationship_uid uid,jsonb_build_object(
   'relationshipUid',r.relationship_uid,'parentSessionId',r.parent_session_id,'childSessionId',r.child_session_id,
   'childModel',r.child_model,'childAgentName',r.child_agent_name,'childAgentType',r.child_agent_type) edge
  FROM sessions.session_directory_relationships r WHERE r.org_id=v_org AND r.workspace_id=p_workspace_id
    AND r.harness=p_harness AND r.parent_session_id=p_session_id ORDER BY r.relationship_uid LIMIT 501) children;
 IF jsonb_array_length(v_children)>500 THEN v_children:=v_children-500; END IF;
 v_body:=jsonb_build_object('version',1,'exists',true,
  'identity',jsonb_build_object('orgId',v_org,'workspaceId',p_workspace_id,'source',p_harness,'sessionId',p_session_id),
  'epoch',d.event_projection_epoch::text,'catalogProjectedAt',d.catalog_read_projected_at,
  'modelsComplete',true,'catalog',jsonb_build_object(
    'title',d.title,'gitBranch',d.git_branch,'repoUrl',d.git_repo,'initialCommit',d.initial_commit,
    'projectKey',d.project_key,'projectKeyMethod',d.project_key_method,'models',to_jsonb(v_models),
    'originator',d.originator,'agentVersion',d.agent_version),
  'parents',v_parents,'children',v_children,'truncated',v_truncated);
 RETURN NEXT v_body||jsonb_build_object('fingerprint',encode(sha256(convert_to(v_body::text,'UTF8')),'hex'));RETURN;
END $catalog$;
REVOKE ALL ON FUNCTION sessions.session_directory_desktop_catalog(text,text,text) FROM PUBLIC,anon,authenticated,service_role;
-- Remove default/future-grantee drift at creation; runtime guard/manifest pins it.
DO $acl$ DECLARE grantee text; BEGIN
 FOR grantee IN SELECT r.rolname FROM pg_proc p CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a JOIN pg_roles r ON r.oid=a.grantee
 WHERE p.oid='sessions.session_directory_desktop_catalog(text,text,text)'::regprocedure AND a.grantee<>p.proowner
 LOOP EXECUTE format('REVOKE ALL ON FUNCTION sessions.session_directory_desktop_catalog(text,text,text) FROM %I',grantee); END LOOP;
END $acl$;
GRANT EXECUTE ON FUNCTION sessions.session_directory_desktop_catalog(text,text,text) TO session_directory_search_worker;
