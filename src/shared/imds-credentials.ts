/** Host Node only; IMDS hop-limit1 remains unchanged. No IMDSv1, proxy, endpoint,
 * environment credential, operator-token, or role-assumption fallback.
 * AWS authority: EC2/UserGuide/instance-metadata-security-credentials.html.
 */
import{request as httpRequest}from'node:http';
import{Readable}from'node:stream';
import type{AwsCredentials}from'../../packages/object-store/src/sigv4.ts';
const ORIGIN='http://169.254.169.254';
const no=(code:string):never=>{throw Object.assign(Error('managed_imds_'+code),{code:'managed_imds_'+code});};
/** A direct fixed link-local socket; environment/global fetch proxies cannot
 * receive the metadata token. Tests may inject a fetcher; process entry cannot. */
const directMetadataFetch=(async(input:Parameters<typeof fetch>[0],init:RequestInit={})=>{
 const u=new URL(String(input));if(u.origin!==ORIGIN||u.search||u.hash)no('transport_target');
 return new Promise<Response>((resolve,reject)=>{
  const req=httpRequest({host:'169.254.169.254',port:80,path:u.pathname,method:init.method??'GET',
   headers:Object.fromEntries(new Headers(init.headers)),agent:false,maxHeaderSize:16384,signal:init.signal??undefined},res=>{
   resolve(new Response(Readable.toWeb(res)as ReadableStream<Uint8Array>,{status:res.statusCode??500}));
  });req.on('error',reject);req.end();
 });
})as typeof fetch;
export interface ImdsIdentity{instanceId:string;accountId:string;region:string;instanceProfileArn:string;roleName:string}
async function text(response:Response,max:number,signal:AbortSignal):Promise<string>{
 if(!response.ok)no('http_'+response.status);if(!response.body)no('empty_body');
 const reader=response.body!.getReader(),parts:Uint8Array[]=[];let size=0;
 try{for(;;){signal.throwIfAborted();const next=await new Promise<Awaited<ReturnType<typeof reader.read>>>((resolve,reject)=>{
  const abort=()=>{reject(signal.reason??Error('managed_imds_abort'));void reader.cancel().catch(()=>{});};
  signal.addEventListener('abort',abort,{once:true});reader.read().then(v=>{signal.removeEventListener('abort',abort);signal.aborted?abort():resolve(v)},e=>{signal.removeEventListener('abort',abort);reject(e)});if(signal.aborted)abort();
 });if(next.done)break;size+=next.value.byteLength;if(size>max)no('body_bound');parts.push(next.value);}}
 finally{void reader.cancel().catch(()=>{});try{reader.releaseLock()}catch{}}
 const all=new Uint8Array(size);let at=0;for(const p of parts){all.set(p,at);at+=p.byteLength;}return new TextDecoder('utf-8',{fatal:true}).decode(all);
}
export function imdsCredentials(expected:ImdsIdentity,options:{fetcher?:typeof fetch;now?:()=>number;timeoutMs?:number}={}): (signal?:AbortSignal)=>Promise<AwsCredentials>{
 if(!/^i-[a-f0-9]{17}$/.test(expected.instanceId)||!/^\d{12}$/.test(expected.accountId)||!/^us-east-1$/.test(expected.region)||
  !/^[A-Za-z0-9_+=,.@-]{1,64}$/.test(expected.roleName)||
  !new RegExp('^arn:aws:iam::'+expected.accountId+':instance-profile/[A-Za-z0-9_+=,.@/-]{1,512}$').test(expected.instanceProfileArn))no('identity_config');
 const fetcher=options.fetcher??directMetadataFetch,now=options.now??Date.now,budget=options.timeoutMs??2000;
 if(!Number.isInteger(budget)||budget<1||budget>5000)no('budget');
 let cached:{credentials:AwsCredentials;expires:number;verified:number}|undefined,inflight:Promise<AwsCredentials>|undefined;
 const refresh=async(caller?:AbortSignal)=>{
  const signal=AbortSignal.any([AbortSignal.timeout(budget),...(caller?[caller]:[])]);
  const request=async(path:string,init:RequestInit,max=16384)=>{
   signal.throwIfAborted();
   const response=await new Promise<Response>((resolve,reject)=>{
    const abort=()=>reject(signal.reason??Error('managed_imds_abort'));
    signal.addEventListener('abort',abort,{once:true});
    Promise.resolve().then(()=>fetcher(ORIGIN+path,{...init,redirect:'error',signal})).then(r=>{
     signal.removeEventListener('abort',abort);if(signal.aborted){abort();void r.body?.cancel().catch(()=>{});}else resolve(r);
    },e=>{signal.removeEventListener('abort',abort);reject(e)});if(signal.aborted)abort();
   });signal.throwIfAborted();return text(response,max,signal);
  };
  const token=await request('/latest/api/token',{method:'PUT',headers:{'X-aws-ec2-metadata-token-ttl-seconds':'300'}},1024);
  if(!token||/[\r\n]/.test(token))no('token');
  const headers={'X-aws-ec2-metadata-token':token};
  const get=(path:string,max?:number)=>request(path,{method:'GET',headers},max);
  const json=(s:string)=>{try{return JSON.parse(s)}catch{return no('json')}};
  const document=json(await get('/latest/dynamic/instance-identity/document'));
  if(!document||typeof document!=='object'||document.instanceId!==expected.instanceId||document.accountId!==expected.accountId||document.region!==expected.region)no('instance_identity');
  const info=json(await get('/latest/meta-data/iam/info'));
  if(!info||typeof info!=='object'||info.Code!=='Success'||info.InstanceProfileArn!==expected.instanceProfileArn)no('profile_identity');
  const role=(await get('/latest/meta-data/iam/security-credentials/',1024)).trim();
  if(role!==expected.roleName)no('role_identity');
  const v=json(await get('/latest/meta-data/iam/security-credentials/'+encodeURIComponent(role)));
  if(!v||typeof v!=='object')no('credential_shape');
  const expires=Date.parse(v.Expiration);
  if(v.Code!=='Success'||v.Type!=='AWS-HMAC'||typeof v.AccessKeyId!=='string'||!/^ASIA[A-Z0-9]{16}$/.test(v.AccessKeyId)||
   typeof v.SecretAccessKey!=='string'||v.SecretAccessKey.length<16||v.SecretAccessKey.length>256||typeof v.Token!=='string'||!v.Token||v.Token.length>8192||
   !Number.isFinite(expires)||expires<=now()+300000)no('credential_shape');
  signal.throwIfAborted();const credentials={accessKeyId:v.AccessKeyId,secretAccessKey:v.SecretAccessKey,sessionToken:v.Token};
  cached={credentials,expires,verified:now()};return {...credentials};
 };
 return async(signal)=>{
  signal?.throwIfAborted();
  if(cached&&now()>=cached.verified&&now()-cached.verified<300000&&cached.expires>now()+300000)return{...cached.credentials};
  if(!inflight)inflight=refresh(signal).finally(()=>{inflight=undefined;});
  const result=await inflight;signal?.throwIfAborted();return{...result};
 };
}
