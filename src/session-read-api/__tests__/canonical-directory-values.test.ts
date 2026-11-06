import {expect,test} from "bun:test";
import {canonicalDirectoryValues,directoryOpeningPrefix} from "../canonical-directory-values.ts";
import {directoryEventsFromProjection} from "../../hoopdb/session-directory-events.ts";
const identity={orgId:"org",workspaceId:"ws"};
const event={session_id:"session:original",source:"claude",machine_id:"machine:original",kind:"prompt",event_id:"event",ts:"2026-11-01T00:00:00.123456Z",_index_fingerprint:"s3abcdef12345678",content:"\u00a0\uFEFF  hello\n\tworld "+ "😀".repeat(600),project_id:"/private/project"};
test("source-attested display keeps full identities, microseconds, Unicode prefix and path; deduplicates both roles",()=>{
 const metadata=directoryEventsFromProjection([event]);
 const rows=canonicalDirectoryValues(identity,metadata,[event]);
 expect(rows.length).toBe(1);
 expect(rows[0]!.row.event).toEqual(metadata.openingEvent);
 expect(rows[0]!.row.event).toEqual(metadata.projectEvent);
 expect(rows[0]!.row.opening_prefix).toBe("hello world "+"😀".repeat(488));
 expect(rows[0]!.row.project_id).toBe("/private/project");
 expect(rows[0]!.row.org_id).toBe("org");
});
test.each(["fingerprint","time","machine","missing"])("stale %s cannot satisfy the admitted locator",mode=>{
 const metadata=directoryEventsFromProjection([event]);
 const changed={...event,...(mode==="fingerprint"?{_index_fingerprint:"s3bbbbbbbbbbbbbb"}:mode==="time"?{ts:"2026-11-01T00:00:00.123457Z"}:mode==="machine"?{machine_id:"machine_original"}:{})};
 expect(()=>canonicalDirectoryValues(identity,metadata,mode==="missing"?[]:[changed])).toThrow("directory_display_missing");
});
test("empty locator inventory is explicit; oversized paths refuse without truncation",()=>{
 expect(canonicalDirectoryValues(identity,directoryEventsFromProjection([]),[])).toEqual([]);
 const row={...event,project_id:"/"+"x".repeat(1024*1024)};
 expect(()=>canonicalDirectoryValues(identity,directoryEventsFromProjection([row]),[row])).toThrow("directory_display_bound");
});

test("bounded scanner matches the legacy normalized Unicode prefix including boundary/trailing whitespace",()=>{
 const cases=["","  \u00a0 ","x".repeat(499)+" \t😀tail","x".repeat(499)+" \t","😀".repeat(499)+"\uFEFFend","\u0085 unchanged\n text",...Array.from({length:40},(_,i)=>("a😀\n\u2007b ".repeat(i*19)))];
 for(const text of cases)expect(directoryOpeningPrefix(text)).toBe([...text.replace(/\s+/gu," ").trim()].slice(0,500).join(""));
});
