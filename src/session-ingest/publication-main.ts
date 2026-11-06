/** Host Node entry. SIGTERM cancels owned IO/queries; GC is separately armed so
 * stopping publication never turns off cleanup of already admitted private bytes.
 */
import{publicationEnabled,runCanonicalPublicationRuntime}from'./publication-runtime.ts';
export async function publicationMain(args=process.argv.slice(2),env=process.env){
 if(args.some(x=>!['--gc','--continuous'].includes(x)))throw Error('publication_arguments');
 const mode=args.includes('--gc')?'gc':'publish';
 if(!publicationEnabled(env,mode))return{enabled:false};
 const controller=new AbortController(),stop=()=>controller.abort();
 process.once('SIGTERM',stop);process.once('SIGINT',stop);
 let cycles=0;
 try{do{
  controller.signal.throwIfAborted();
  const counts=await runCanonicalPublicationRuntime(env,mode,{},controller.signal);cycles++;
  console.log(JSON.stringify({kind:'canonical-publication',mode,healthy:true,...counts}));
  if(!args.includes('--continuous'))break;
  if('examined'in counts&&Number(counts.examined)>0)continue;
  await new Promise<void>((resolve,reject)=>{const abort=()=>{clearTimeout(timer);reject(controller.signal.reason)};
   const timer=setTimeout(()=>{controller.signal.removeEventListener('abort',abort);resolve()},5000);
   controller.signal.addEventListener('abort',abort,{once:true});if(controller.signal.aborted)abort();
  });
 }while(!controller.signal.aborted);return{enabled:true,cycles};}
 finally{controller.abort();process.removeListener('SIGTERM',stop);process.removeListener('SIGINT',stop);}
}
if(import.meta.main)publicationMain().catch(error=>{
 const raw=error?.code;const code=typeof raw==='string'&&/^[A-Za-z0-9_]{1,64}$/.test(raw)?raw:'publication_failed';
 console.error(JSON.stringify({kind:'canonical-publication',healthy:false,code}));process.exitCode=1;
});

// Offline bundle proofs import the real runtime with injected owned factories.
// The CLI never accepts factories, credentials, or endpoints from argv/input.
export{runCanonicalPublicationRuntime}from'./publication-runtime.ts';
