import{test,expect}from'bun:test';
import{imdsCredentials,type ImdsIdentity}from'../../shared/imds-credentials.ts';
const identity:ImdsIdentity={instanceId:'i-0123456789abcdef0',accountId:'111122223333',region:'us-east-1',instanceProfileArn:'arn:aws:iam::111122223333:instance-profile/owned-fixture-profile',roleName:'owned-fixture-role'};
function fixture(change?:(path:string,value:any)=>any){let now=1762243200000;const calls:{path:string;init:RequestInit}[]=[];
 const fetcher=(async(input:any,init:RequestInit)=>{const u=new URL(String(input));expect(u.origin).toBe('http://169.254.169.254');expect(init.redirect).toBe('error');expect(init.signal).toBeDefined();calls.push({path:u.pathname,init});
  if(u.pathname==='/latest/api/token'){expect(init.method).toBe('PUT');expect((init.headers as any)['X-aws-ec2-metadata-token-ttl-seconds']).toBe('300');return new Response('owned-token');}
  expect(init.method).toBe('GET');expect((init.headers as any)['X-aws-ec2-metadata-token']).toBe('owned-token');
  const values:any={
   '/latest/dynamic/instance-identity/document':{instanceId:identity.instanceId,accountId:identity.accountId,region:identity.region},
   '/latest/meta-data/iam/info':{Code:'Success',InstanceProfileArn:identity.instanceProfileArn},
   '/latest/meta-data/iam/security-credentials/':identity.roleName,
   ['/latest/meta-data/iam/security-credentials/'+identity.roleName]:{Code:'Success',Type:'AWS-HMAC',AccessKeyId:'ASIA'+'X'.repeat(16),SecretAccessKey:'synthetic-secret-key-only',Token:'synthetic-session-token',Expiration:new Date(now+3600000).toISOString()}
  };const v=change?change(u.pathname,values[u.pathname]):values[u.pathname];return new Response(typeof v==='string'?v:JSON.stringify(v));}) as typeof fetch;
 const get=imdsCredentials(identity,{fetcher,now:()=>now});return{get,calls,setNow:(n:number)=>now=n,now:()=>now};}
test('pinned IMDSv2 only, concurrent refresh is shared; role credentials reused and reverified every five minutes',async()=>{
 const f=fixture();const[a,b]=await Promise.all([f.get(),f.get()]);expect(a).toEqual(b);expect(f.calls).toHaveLength(5);expect(a.sessionToken).toBe('synthetic-session-token');
 a.secretAccessKey='caller-mutated';expect((await f.get()).secretAccessKey).toBe('synthetic-secret-key-only');expect(f.calls).toHaveLength(5);
 f.setNow(f.now()+300001);await f.get();expect(f.calls).toHaveLength(10);
});
test('wrong instance/account/profile/role, persistent key or expired credentials refuse without fallback',async()=>{
 const cases:[string,(v:any)=>any][]=[
  ['/latest/dynamic/instance-identity/document',v=>({...v,instanceId:'i-00000000000000000'})],
  ['/latest/dynamic/instance-identity/document',v=>({...v,accountId:'111111111111'})],
  ['/latest/meta-data/iam/info',v=>({...v,InstanceProfileArn:identity.instanceProfileArn+'-other'})],
  ['/latest/meta-data/iam/security-credentials/',()=>identity.roleName+'\nforeign-role'],
  ['/latest/meta-data/iam/security-credentials/'+identity.roleName,v=>({...v,AccessKeyId:'AKIA'+'X'.repeat(16)})],
  ['/latest/meta-data/iam/security-credentials/'+identity.roleName,v=>({...v,Expiration:new Date(0).toISOString()})]
 ];for(const[path,change]of cases){const f=fixture((p,v)=>p===path?change(v):v);await expect(f.get()).rejects.toThrow('managed_imds_');expect(f.calls.every(c=>c.init.method==='PUT'||(c.init.headers as any)['X-aws-ec2-metadata-token']==='owned-token')).toBe(true);}
});
test('failed token never performs an unauthed metadata GET and never reads environment credentials',async()=>{
 let calls=0;const get=imdsCredentials(identity,{fetcher:(async()=>{calls++;return new Response('denied',{status:403})})as typeof fetch});
 await expect(get()).rejects.toMatchObject({code:'managed_imds_http_403'});expect(calls).toBe(1);
});
test('oversized metadata body refuses before credentials and stalled body respects deadline',async()=>{
 const get=imdsCredentials(identity,{fetcher:(async()=>new Response('x'.repeat(1025)))as typeof fetch});await expect(get()).rejects.toMatchObject({code:'managed_imds_body_bound'});
 const stalled=imdsCredentials(identity,{timeoutMs:15,fetcher:(async()=>new Response(new ReadableStream({start(){}})))as typeof fetch});
 await expect(stalled()).rejects.toBeDefined();
});

test('a transport ignoring abort before headers is still bounded without metadata fallback',async()=>{
 const get=imdsCredentials(identity,{timeoutMs:15,fetcher:(async()=>new Promise(()=>{}))as typeof fetch});
 await expect(get()).rejects.toBeDefined();
});
