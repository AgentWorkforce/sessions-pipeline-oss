import { fileURLToPath } from "node:url";
/**
 * Re-sync src/session-ingest/vendor/delivery from a relayhistory-cloud
 * checkout at a commit:
 *
 *   bun scripts/vendor-relayhistory-delivery.ts <relayhistory-cloud checkout> <commit> [--check]
 *
 * Copies delivery-contracts.ts and session-image.ts verbatim and rebuilds
 * prepare.ts from its local header plus the recorded line ranges of
 * delivery.ts, then rewrites SOURCE.json. The ranges are not moved
 * automatically: if delivery.ts changed shape, the script refuses and the
 * ranges are chosen again by hand (the Neon half must stay out). `--check`
 * writes nothing and exits 1 on any difference.
 */
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const VENDOR = join(fileURLToPath(new URL(".", import.meta.url)), "../src/session-ingest/vendor/delivery");
const SRC_PATH = "packages/relayhistory/src/lib";
const sha = (text: string) => createHash("sha256").update(text).digest("hex");

const [checkout, commit, flag] = process.argv.slice(2);
if (!checkout || !commit) {
  console.error("usage: bun scripts/vendor-relayhistory-delivery.ts <relayhistory-cloud checkout> <commit> [--check]");
  process.exit(2);
}
const check = flag === "--check";
const show = (file: string) => execFileSync("git", ["-C", checkout, "show", `${commit}:${SRC_PATH}/${file}`], { encoding: "utf8", maxBuffer: 16 << 20 });

const source = JSON.parse(readFileSync(join(VENDOR, "SOURCE.json"), "utf8"));
const outputs = new Map<string, string>();
const files: Record<string, string> = {};
for (const name of Object.keys(source.files)) {
  const text = show(name);
  outputs.set(name, text);
  files[name] = sha(text);
}

const prepare = readFileSync(join(VENDOR, "prepare.ts"), "utf8");
const header = prepare.slice(0, prepare.indexOf("// ---- relayhistory delivery.ts lines "));
const delivery = show("delivery.ts").split("\n");
const ranges: Record<string, string> = {};
let body = header;
for (const range of Object.keys(source.extracted["prepare.ts"].ranges)) {
  const [a, b] = range.split("-").map(Number) as [number, number];
  const text = delivery.slice(a - 1, b).join("\n") + "\n";
  if (/drizzle|commitDelivery|DeliveryDb/.test(text)) {
    console.error(`range ${range} now reaches delivery.ts's Neon half: choose the ranges again by hand`);
    process.exit(1);
  }
  ranges[range] = sha(text);
  body += `// ---- relayhistory delivery.ts lines ${range} ----\n${text}`;
}
outputs.set("prepare.ts", body);

const record = {
  ...source,
  commit,
  files,
  extracted: { "prepare.ts": { from: "delivery.ts", from_sha256: sha(delivery.join("\n")), ranges } },
};
outputs.set("SOURCE.json", JSON.stringify(record, null, 2) + "\n");

let differs = false;
for (const [name, text] of outputs) {
  const current = readFileSync(join(VENDOR, name), "utf8");
  if (current === text) continue;
  differs = true;
  console.log(`${check ? "differs" : "updated"}: ${name}`);
  if (!check) writeFileSync(join(VENDOR, name), text);
}
if (check && differs) process.exit(1);
console.log(differs ? "re-synced; review the diff" : "in sync");
