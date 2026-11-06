#!/usr/bin/env bash
# Pin the RelayHistory (Neon) schema migrations from AgentWorkforce/relayhistory-cloud
# into vendor/relayhistory-cloud/migrations, for the schema-parity test.
# Usage: scripts/sync-relayhistory-schema.sh <path-to-relayhistory-cloud-checkout> <ref>
set -euo pipefail
src=${1:?relayhistory-cloud checkout}
ref=${2:?git ref}
dir=packages/relayhistory/db/migrations
dest=vendor/relayhistory-cloud/migrations
sha=$(git -C "$src" rev-parse --verify "$ref^{commit}")
rm -rf "$dest"
mkdir -p "$dest"
git -C "$src" ls-tree --name-only "$sha" "$dir/" | while read -r path; do
  case "$path" in *.sql) git -C "$src" show "$sha:$path" > "$dest/${path##*/}" ;; esac
done
# The runner builds hot-table indexes CONCURRENTLY after the migrations commit
# (CONCURRENT_INDEXES in scripts/apply-neon-migration.mjs). Extract that literal
# alone, so its imports (Neon driver, ws) are never loaded.
git -C "$src" show "$sha:packages/relayhistory/scripts/apply-neon-migration.mjs" |
  awk '/^export const CONCURRENT_INDEXES = \[/{on=1; sub(/^export const CONCURRENT_INDEXES = /, "")} on{print} on && /^\];/{exit}' |
  node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const v=new Function("return "+s.replace(/;\s*$/,""))();process.stdout.write(JSON.stringify(v.map(({name,definition,extension})=>({name,definition,extension:extension??null})),null,2)+"\n")})' \
  > vendor/relayhistory-cloud/concurrent-indexes.json
printf 'AgentWorkforce/relayhistory-cloud %s:%s\n' "$sha" "$dir" > vendor/relayhistory-cloud/SOURCE
echo "pinned $(ls "$dest" | wc -l | tr -d ' ') migrations at $sha"
