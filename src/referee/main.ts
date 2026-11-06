import { lockerManifestReader } from "../shared/locker.ts";
import { runWorker } from "../shared/worker.ts";
import { refereeHandler } from "./handler.ts";

await runWorker({
  service: "referee",
  stages: ["status-classify"],
  handlers: (db) => ({ "status-classify": refereeHandler({ db, reader: lockerManifestReader() }) }),
});
