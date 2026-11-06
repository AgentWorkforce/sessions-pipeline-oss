/**
 * Referee: the session-state classifier. Consumes `status-classify`. Rules only
 * by default (no model call): closed raw stream = done; no activity for
 * REFEREE_ABANDON_AFTER_MINUTES = abandoned; otherwise active. Writes the
 * verdict through the vendored `sessions.set_session_state` (monotonic on
 * classified_at) and keeps every verdict in `session_verdicts`.
 */
import {
  PipelineError,
  type ManifestReader,
  type RawPointer,
  type StageHandler,
} from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";
import { ctxOf } from "../shared/enrich.ts";
import { integer } from "../shared/env.ts";
import { log } from "../shared/log.ts";
import { projectSessionDirectory } from "../hoopdb/session-directory-projector.ts";

export type Verdict = {
  state: "active" | "done" | "abandoned" | "unknown";
  rule: string;
};

export function classify(input: {
  closed: boolean;
  lastActivity: Date;
  endedAt?: Date;
  now: Date;
  abandonAfterMinutes: number;
}): Verdict {
  if (input.closed || input.endedAt)
    return {
      state: "done",
      rule: input.closed ? "raw_closed" : "ended_at_present",
    };
  const idleMinutes =
    (input.now.getTime() - input.lastActivity.getTime()) / 60_000;
  if (idleMinutes > input.abandonAfterMinutes)
    return { state: "abandoned", rule: "idle_beyond_threshold" };
  return { state: "active", rule: "recent_activity" };
}

export function refereeHandler(deps: {
  db: HoopDb;
  reader: ManifestReader;
  now?: () => Date;
}): StageHandler {
  const now = deps.now ?? (() => new Date());
  const abandonAfterMinutes = integer("REFEREE_ABANDON_AFTER_MINUTES", 24 * 60);
  return async (message, recorder) => {
    const pointer = message.payload as RawPointer;
    if (!pointer || typeof pointer.manifestKey !== "string")
      throw new PipelineError(
        "invalid_pointer",
        "status-classify payload is not a raw pointer",
        false,
      );
    const read = await deps.reader.read(pointer.manifestKey);
    if (!read) return { status: "skipped", reason: "manifest_missing" };
    const m = read.manifest;
    const s = m.session;
    const lastActivity = new Date(s.ended_at ?? m.raw.updated_at);
    const verdict = classify({
      closed: m.raw.state === "closed",
      lastActivity,
      ...(s.ended_at ? { endedAt: new Date(s.ended_at) } : {}),
      now: now(),
      abandonAfterMinutes,
    });
    const classifiedAt = now();
    const ctx = ctxOf(message);
    const { rows } = await deps.db.query<{ set_session_state: boolean }>(
      "SELECT sessions.set_session_state($1,$2,$3,$4,$5,$6,$7::timestamptz)",
      [
        message.ctx.orgId,
        s.workspace_id,
        s.harness,
        s.session_id,
        s.machine_id,
        verdict.state,
        classifiedAt.toISOString(),
      ],
    );
    // set_session_state updates only the pointer; re-project the logical session so
    // the directory's state (and its p_state filter) follows the classification.
    if (rows[0]?.set_session_state)
      await projectSessionDirectory(deps.db, {
        orgId: message.ctx.orgId,
        workspaceId: s.workspace_id,
        harness: s.harness,
        sessionId: s.session_id,
      });
    await deps.db.query(
      `INSERT INTO sessions.session_verdicts
         (org_id, workspace_id, harness, session_id, machine_id, session_uuid, trace_id, revision, state, rule, model_id, classified_at)
       VALUES ($1,$2,$3,$4,$5,$6::uuid,$7::uuid,$8,$9,$10,'rules/v1',$11::timestamptz)
       ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id, trace_id, revision) DO NOTHING`,
      [
        message.ctx.orgId,
        s.workspace_id,
        s.harness,
        s.session_id,
        s.machine_id,
        ctx.session_uuid ?? null,
        m.trace_id,
        m.revision,
        verdict.state,
        verdict.rule,
        classifiedAt.toISOString(),
      ],
    );
    recorder.log.info("verdict", { state: verdict.state, rule: verdict.rule });
    log.info("referee verdict", {
      stage: "status-classify",
      state: verdict.state,
      rule: verdict.rule,
      model_id: "rules/v1",
      trace_id: m.trace_id,
      revision: m.revision,
      session_uuid: ctx.session_uuid,
      ok: rows[0]?.set_session_state ?? false,
    });
  };
}
