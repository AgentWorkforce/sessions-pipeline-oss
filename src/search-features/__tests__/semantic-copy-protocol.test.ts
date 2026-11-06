import {expect,test} from "bun:test";
import {semanticCheckpoint,semanticSha256,semanticDataHashInput,semanticAuthorityHashInput,semanticStatusVersion,parseSemanticProfile,foldSemanticQuery,semanticCursorBindingInput} from "../semantic-copy-protocol.ts";
test("authority generation fences writes while stable data hashes preserve unchanged cursor identity",async()=>{
 const v={sourceId:"legacy-main",kind:"event" as const,sourceAuthorityHash:"a".repeat(64),targetHash:"b".repeat(64),profileHash:"c".repeat(64),stateHash:"d".repeat(64)};
 const data=await semanticSha256(semanticDataHashInput(v));expect(data).toMatch(/^[a-f0-9]{64}$/);expect(await semanticSha256(semanticDataHashInput({...v}))).toBe(data);
 expect(await semanticSha256(semanticAuthorityHashInput(data,"5",true))).not.toBe(await semanticSha256(semanticAuthorityHashInput(data,"6",true)));
 expect(semanticStatusVersion("9007199254740993",true)).toBe("18014398509481986");expect(semanticStatusVersion("9007199254740993",false)).toBe("18014398509481987");
});
test("checkpoint validation refuses invented completeness or reset baseline",()=>{
 expect(semanticCheckpoint({phase:"baseline",baselineH:null,baselineCursor:null,checkpoint:"0",observedRevision:"0",polled:false}).baselineH).toBeNull();
 for(const v of [{phase:"journal",baselineH:null,baselineCursor:null,checkpoint:"5",observedRevision:"5",polled:true},{phase:"baseline",baselineH:"5",baselineCursor:null,checkpoint:"5",observedRevision:"5",polled:true},{phase:"journal",baselineH:"5",baselineCursor:null,checkpoint:"6",observedRevision:"5",polled:true}])expect(()=>semanticCheckpoint(v)).toThrow("feature_checkpoint_invalid");
});
test("fold profile preserves source mapping and refuses duplicate or unbounded metadata",()=>{
 const json=JSON.stringify({source:{datcollate:"en_US.utf8",datctype:"en_US.utf8",datlocprovider:"c"},entries:[["İ","i"]]});expect(foldSemanticQuery(parseSemanticProfile(json),"İ_100%")).toBe("i_100%");
 expect(()=>parseSemanticProfile(json.replace('[["İ","i"]]','[["İ","i"],["İ","x"]]'))).toThrow("feature_profile_invalid");
 expect(()=>parseSemanticProfile(" ".repeat(262145))).toThrow("feature_profile_bound");
});

test("builtin profile requires exact attested C.UTF-8 locale metadata and preserves simple source sigma",()=>{const profile={source:{datcollate:"C.UTF-8",datctype:"C.UTF-8",datlocprovider:"b",datlocale:"C.UTF-8"},entries:[["Ο","ο"],["Σ","σ"]]};expect(foldSemanticQuery(parseSemanticProfile(JSON.stringify(profile)),"ΟΣ_%")).toBe("οσ_%");for(const patch of [{datlocale:""},{datcollate:"en_US.utf8"},{datctype:"C"},{datlocprovider:"i"}])expect(()=>parseSemanticProfile(JSON.stringify({...profile,source:{...profile.source,...patch}}))).toThrow("feature_profile_invalid")});

test("frozen rank cursor identity survives unrelated progress and changes only for deployment/source/profile",()=>{const a={sourceId:"legacy-main",sourceAuthorityHash:"a".repeat(64),targetHash:"b".repeat(64),profileHash:"c".repeat(64)},b={...a,dataHash:"changed",generation:"99",checkpoint:"999"};expect(semanticCursorBindingInput(a)).toBe(semanticCursorBindingInput(b));expect(semanticCursorBindingInput({...a,profileHash:"d".repeat(64)})).not.toBe(semanticCursorBindingInput(a))});
