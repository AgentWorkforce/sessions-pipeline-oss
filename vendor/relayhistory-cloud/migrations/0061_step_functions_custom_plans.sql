-- Plan the rollout step functions' cursor queries for each call's own cursor.
--
-- These steps find their next work with `progress.cursor IS NULL OR (key) >
-- cursor`. plpgsql caches a function's query plans per session; from a
-- session's sixth call Postgres may switch to a generic plan, which cannot use
-- the cursor as an index bound and walks the index from its start. On a copy of
-- production (2026-10-20) one delivery_sidechain_cleanup_step call took 1,115 s
-- on that plan against ~0.1 s on a custom one, failing the deploy rollout with
-- 57014. scripts/rollout-delivery-projection.mjs sets the same mode per batch
-- (#153); setting it on the functions keeps any caller off the generic plan.
--
-- No data changes. Other *_step functions branch on the cursor with IF/ELSE or
-- dynamic EXECUTE, so they are already planned per cursor.
ALTER FUNCTION sessions.delivery_catalog_backfill_step(integer)
  SET plan_cache_mode = force_custom_plan;
ALTER FUNCTION sessions.delivery_activity_reproject_step(integer)
  SET plan_cache_mode = force_custom_plan;
ALTER FUNCTION sessions.delivery_sidechain_cleanup_step(integer)
  SET plan_cache_mode = force_custom_plan;
