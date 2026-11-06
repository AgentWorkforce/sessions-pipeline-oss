/** Compact display projection from the same verified full canonical event union.
 * Supabase stores only locators. Text/paths remain in SHA-bound S3 artifacts.
 */
import { canonicalTimestamp } from "../../packages/pipeline/src/lib/pipeline/relayhistory-projection.ts";
import type { DirectoryEvents, EventLocator } from "../hoopdb/session-directory-events.ts";
export const DIRECTORY_VALUES_TABLE = "session_directory_event_values";
const key = (event: EventLocator) => JSON.stringify([event.source,event.session_id,event.machine_id,event.kind,event.event_id,event.ts,event.fingerprint]);
/** Equivalent to normalized trim/substrUTF8(500), without copying a large tail. */
export function directoryOpeningPrefix(content:string):string {
  let result="",count=0,pendingSpace=false;
  for(const char of content){
    if(/\s/u.test(char)){pendingSpace=count>0;continue;}
    if(pendingSpace){result+=" ";count++;pendingSpace=false;if(count===500)break;}
    result+=char;if(++count===500)break;
  }
  return result;
}
export function canonicalDirectoryValues(
  identity: {orgId:string;workspaceId:string},
  events: DirectoryEvents,
  rows: Record<string,unknown>[],
): Array<{table:string;row:Record<string,unknown>}> {
  const wanted = new Map([events.openingEvent,events.projectEvent].filter((v):v is EventLocator=>v!==null).map(event=>[key(event),event]));
  const found = new Map<string,Record<string,unknown>>();
  for (const row of rows) {
    const event = {source:row.source,session_id:row.session_id,machine_id:row.machine_id,kind:row.kind,event_id:row.event_id,ts:canonicalTimestamp(String(row.ts)),fingerprint:row._index_fingerprint} as EventLocator;
    const at = key(event); if (!wanted.has(at)) continue;
    if (found.has(at)) throw new Error("directory_display_duplicate");
    // Matches legacy CH replaceRegexpAll/trimBoth/substrUTF8 before condenseTurn.
    const opening = events.openingEvent && at === key(events.openingEvent) && typeof row.content === "string" ? directoryOpeningPrefix(row.content) : "";
    const value = {org_id:identity.orgId,workspace_id:identity.workspaceId,source:event.source,session_id:event.session_id,event,opening_prefix:opening,project_id:events.projectEvent && at === key(events.projectEvent) && typeof row.project_id === "string" ? row.project_id : null};
    if (new TextEncoder().encode(JSON.stringify(value)).length > 1024*1024) throw new Error("directory_display_bound");
    found.set(at,value);
  }
  if (found.size !== wanted.size) throw new Error("directory_display_missing");
  return [...found.values()].map(row=>({table:DIRECTORY_VALUES_TABLE,row}));
}
