#!/bin/sh
# SERVICE selects the process. Unknown values fail loudly.
set -e
case "${SERVICE:-}" in
  hoopdb-migrate)   exec bun run /app/src/hoopdb/migrate.ts ;;
  locker-init)      exec bun run /app/src/locker/init.ts ;;
  alleyoop-ingest)  exec bun run /app/src/alleyoop/ingest.ts ;;
  alleyoop-courier) exec bun run /app/src/alleyoop/courier.ts ;;
  hoopdb-indexer)   exec bun run /app/src/hoopdb/indexer.ts ;;
  referee)          exec bun run /app/src/referee/main.ts ;;
  bob-costas)       exec bun run /app/src/bob-costas/main.ts ;;
  session-brief)    exec bun run /app/src/session-brief/main.ts ;;
  *) echo "96bulls: unknown SERVICE='${SERVICE:-}'" >&2; exit 2 ;;
esac
