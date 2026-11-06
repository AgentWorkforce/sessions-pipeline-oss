import { scopeLockKey, type ScopeLock } from "../pointer.ts";

/** An in-process stand-in for the Supabase advisory lock: one holder per scope at a time. */
export function memoryLock(): ScopeLock {
  const tails = new Map<string, Promise<unknown>>();
  return async (scope, work) => {
    const key = scopeLockKey(scope);
    const prior = tails.get(key) ?? Promise.resolve();
    const run = prior.then(work, work);
    tails.set(key, run.catch(() => {}));
    return run;
  };
}
