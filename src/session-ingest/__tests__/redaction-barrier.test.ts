import{test,expect}from'bun:test';
import{createHash}from'node:crypto';
import{ensureExactRedactionBarrier}from'../../shared/redaction-barrier.ts';
import{ObjectStoreError,type ObjectStore}from'../../shared/vendor.ts';
const bytes=new TextEncoder().encode('private-original'),marker=new TextEncoder().encode('{"redacted":true}');
const proof={key:'safe/private.bin',bytes:bytes.length,sha256:createHash('sha256').update(bytes).digest('hex'),marker};
function store(initial:Uint8Array|undefined,missing403=false){let value=initial,version=1;const opts:any[]=[];
 const denied=new ObjectStoreError('unauthorized','s3','denied',{status:403});
 const impl={backend:'s3',async getObject(){if(!value)throw missing403?denied:new ObjectStoreError('not_found','s3','missing');
  const b=value.slice();return{meta:{version:{eTag:String(version),version:String(version)}},body:new Blob([b]).stream()};},
 async put(_k:any,b:Uint8Array,o:any){opts.push(o);if(o.ifNoneMatch&&value)throw new ObjectStoreError('already_exists','s3','exists');
  if(o.ifMatch&&o.ifMatch.eTag!==String(version))throw new ObjectStoreError('precondition_failed','s3','changed');value=b.slice();version++;return{};}};
 return{impl:impl as unknown as ObjectStore,opts,denied,get value(){return value},set value(v){value=v;version++;}};
}
test('replaces only exact original version, then returns verified permanent marker version',async()=>{
 const s=store(bytes);expect(await ensureExactRedactionBarrier(s.impl,proof)).toBe('2');expect(s.opts[0].ifMatch.eTag).toBe('1');
 expect(await ensureExactRedactionBarrier(s.impl,proof)).toBe('2');expect(s.opts).toHaveLength(1);
 await expect(s.impl.put(proof.key as never,bytes,{ifNoneMatch:true})).rejects.toMatchObject({code:'already_exists'});
 expect(s.value).toEqual(marker);
});
test('missing GET403 may only create conditionally and must subsequently verify GET version',async()=>{
 const s=store(undefined,true);expect(await ensureExactRedactionBarrier(s.impl,proof)).toBe('2');expect(s.opts).toHaveLength(1);expect(s.opts[0].ifNoneMatch).toBe(true);
});
test('denied existing object never overwritten and preserves denied read error',async()=>{
 const s=store(bytes);s.impl.getObject=async()=>{throw s.denied;};
 await expect(ensureExactRedactionBarrier(s.impl,proof)).rejects.toBe(s.denied);expect(s.value).toEqual(bytes);expect(s.opts[0].ifNoneMatch).toBe(true);
});
test('unrecognized current bytes cannot be replaced even with the right key',async()=>{
 const s=store(new TextEncoder().encode('different-private'));
 await expect(ensureExactRedactionBarrier(s.impl,proof)).rejects.toMatchObject({code:'redaction_barrier_unexpected_content'});expect(s.opts).toHaveLength(0);
});
test('CAS race to an unrelated body refuses instead of overwriting the winning body',async()=>{
 const s=store(bytes),put=s.impl.put.bind(s.impl);let first=true;
 s.impl.put=async(...args)=>{if(first){first=false;s.value=new TextEncoder().encode('unrelated');}return put(...args);};
 await expect(ensureExactRedactionBarrier(s.impl,proof)).rejects.toMatchObject({code:'redaction_barrier_unexpected_content'});expect(s.opts).toHaveLength(1);
});
