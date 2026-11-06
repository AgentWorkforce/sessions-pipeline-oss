import {readFileSync} from "node:fs";
import {PGlite} from "@electric-sql/pglite";
import type {ReadonlyAnalysisSource} from "../analysis-snapshot.ts";
export const LEGACY_ANALYSIS_IDENTITY={orgId:"org",workspaceId:"ws",source:"claude",sessionId:"session"};
export const LEGACY_REQUEST="00000000-0000-4000-8000-000000000001",LEGACY_ANALYSIS="00000000-0000-4000-8000-000000000002",LEGACY_BRIEF="00000000-0000-4000-8000-000000000003";
const request=LEGACY_REQUEST,analysis=LEGACY_ANALYSIS,brief=LEGACY_BRIEF;
export async function legacyAnalysisOracle(){const pg=new PGlite();await pg.exec(`CREATE SCHEMA sessions;
CREATE TABLE sessions.convergence_events(org_id text,session_id text,workspace_id text,source text);
CREATE TABLE sessions.conversation_turns(org_id text,session_id text,workspace_id text,metadata jsonb,source text,session_owner text NOT NULL DEFAULT 'owner');
CREATE TABLE sessions.session_analysis_requests(id uuid,org_id text,workspace_id text,session_id text,status text,last_error text,analysis_job_id uuid,brief_job_id uuid,created_at timestamptz,updated_at timestamptz);
CREATE TABLE sessions.session_analysis_jobs(id uuid,org_id text,session_id text,status text,last_error text,source jsonb,source_provider text,source_fingerprint text,attempts integer,analysis_version text,schema_version integer,summary_version text,model_version text,summary_model_version text,result jsonb,created_at timestamptz,updated_at timestamptz,completed_at timestamptz,workspace_id text);
INSERT INTO sessions.convergence_events VALUES('org','session','ws','claude');`);
 await pg.exec(readFileSync(new URL("./fixtures/analysis-v1-oracle.sql",import.meta.url),"utf8"));
 await pg.query(`INSERT INTO sessions.session_analysis_jobs VALUES($1,'org','session','running',NULL,'{"evidenceWorkspaceIds":["ws"]}','claude','raw',1,'a',1,'s','m','sm',NULL,'2026-11-01','2026-11-01',NULL),($2,'org','session','completed',NULL,'{"evidenceWorkspaceIds":["ws"]}','claude','raw',1,'a',1,'s','m','sm','{"title":"actual title","description":"nested description","arbitrary":{"data":[1,2]}}','2026-11-01','2026-11-01','2026-11-01')`,[analysis,brief]);
 await pg.query(`INSERT INTO sessions.session_analysis_requests VALUES($1,'org','ws','session','resolved',NULL,$2,$3,'2026-11-01','2026-11-01')`,[request,analysis,brief]);
 await pg.exec("ALTER TABLE sessions.session_analysis_jobs ADD COLUMN kind text DEFAULT 'analysis';UPDATE sessions.session_analysis_jobs SET workspace_id='ws'");await pg.query("UPDATE sessions.session_analysis_jobs SET kind='brief' WHERE id=$1",[brief]);
 const source:ReadonlyAnalysisSource={snapshot:work=>pg.transaction(async tx=>{await tx.exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");return work({query:(sql,params)=>tx.query(sql,params)})})};return {pg,source};}
