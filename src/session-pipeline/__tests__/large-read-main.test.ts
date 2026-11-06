import {expect,test} from 'bun:test';
import {largeReadMain} from '../large-read-main.ts';
import type {LargeReadRuntime} from '../large-read-main.ts';
function runtime(pages:Array<{busy:boolean;scanned:number;changed:number;cycleComplete:boolean}>) {
 let scans=0,claims=0,closed=0;
 return { factory:()=>({deps:{db:{query:async(sql:string)=>{
  if(sql.includes('discover_session_large_reads'))return {rows:[{r:pages[Math.min(scans++,pages.length-1)]}]};
  if(sql.includes('claim_session_large_read')){claims++;return {rows:[{job:null}]}};
  throw new Error('unexpected SQL');
 }}} as unknown as LargeReadRuntime['deps'],close:async()=>{closed++}}),counts:()=>({scans,claims,closed})};
}
test('default-off boot and invalid bounds never open runtime or read credentials',async()=>{
 let opened=0;const factory=()=>{opened++;throw new Error('runtime must remain closed')};
 expect(await largeReadMain({},factory)).toMatchObject({enabled:false,processed:0});
 await expect(largeReadMain({SESSION_LARGE_READ_WORKER_ENABLED:'1',SESSION_LARGE_READ_MAX_JOBS:'11'},factory)).rejects.toThrow('job bound');
 expect(opened).toBe(0);
});
test('durable discovery advances before claiming and is bounded to16pages',async()=>{
 const f=runtime([{busy:false,scanned:128,changed:1,cycleComplete:false}]);
 expect(await largeReadMain({SESSION_LARGE_READ_WORKER_ENABLED:'1'},f.factory)).toEqual({enabled:true,processed:0,published:0,failed:0,scanned:2048,cycleComplete:false});
 expect(f.counts()).toEqual({scans:16,claims:16,closed:1});
});
test('a complete unchanged cycle stops without spinning and closes',async()=>{
 const f=runtime([{busy:false,scanned:0,changed:0,cycleComplete:true}]);
 expect(await largeReadMain({SESSION_LARGE_READ_WORKER_ENABLED:'1'},f.factory)).toMatchObject({cycleComplete:true});
 expect(f.counts()).toEqual({scans:2,claims:1,closed:1});
});
test('invalid discovery is refused and the pool is closed',async()=>{
 const f=runtime([{busy:false,scanned:129,changed:0,cycleComplete:false}]);
 await expect(largeReadMain({SESSION_LARGE_READ_WORKER_ENABLED:'1'},f.factory)).rejects.toThrow('invalid discovery');
 expect(f.counts()).toEqual({scans:1,claims:0,closed:1});
});

test('a discovery error (54000 capacity) is logged by code and never blocks claiming queued jobs',async()=>{
 let scans=0,claims=0,closed=0;const lines:any[]=[];
 const capacity=Object.assign(new Error('replay capture: pointer capacity exceeded org_secret/ws_secret'),{code:'54000'});
 const factory=()=>({deps:{db:{query:async(sql:string)=>{
  if(sql.includes('discover_session_large_reads')){scans++;throw capacity}
  if(sql.includes('claim_session_large_read')){claims++;return {rows:[{job:null}]}};
  throw new Error('unexpected SQL');
 }}} as unknown as LargeReadRuntime['deps'],close:async()=>{closed++}});
 const err=process.stderr.write;
 process.stderr.write=((chunk:any,...rest:any[])=>{for(const l of String(chunk).split('\n'))if(l.startsWith('{'))lines.push(JSON.parse(l));return true}) as any;
 let result;try{result=await largeReadMain({SESSION_LARGE_READ_WORKER_ENABLED:'1'},factory).catch(e=>({rejected:e?.code}))}finally{process.stderr.write=err}
 expect(result).toEqual({enabled:true,processed:0,published:0,failed:0,scanned:0,cycleComplete:false,discoveryFailures:2});
 // Claimed after the failed first page; the second failing page stops the loop instead of spinning.
 expect({scans,claims,closed}).toEqual({scans:2,claims:1,closed:1});
 expect(lines).toHaveLength(2);
 for(const l of lines){expect(l).toMatchObject({level:'error',message:'large worker discovery failed',stage:'large-read',error_code:'54000',reason:'replay_capacity_refused'});expect(JSON.stringify(l)).not.toContain('secret')}
});
test('an unclassified discovery error is logged and claims still run',async()=>{
 let claims=0;const job={rows:[{job:null}]};
 const factory=()=>({deps:{db:{query:async(sql:string)=>{
  if(sql.includes('discover_session_large_reads'))throw Object.assign(new Error('ambiguous'),{code:'55000'});
  if(sql.includes('claim_session_large_read')){claims++;return job}
  throw new Error('unexpected SQL');
 }}} as unknown as LargeReadRuntime['deps'],close:async()=>{}});
 const err=process.stderr.write;const lines:any[]=[];
 process.stderr.write=((chunk:any)=>{for(const l of String(chunk).split('\n'))if(l.startsWith('{'))lines.push(JSON.parse(l));return true}) as any;
 let result;try{result=await largeReadMain({SESSION_LARGE_READ_WORKER_ENABLED:'1'},factory).catch(e=>({rejected:e?.code}))}finally{process.stderr.write=err}
 expect(result).toMatchObject({enabled:true,discoveryFailures:2});
 expect(claims).toBe(1);expect(lines[0]).toMatchObject({error_code:'55000',reason:'discovery_failed'});
});
