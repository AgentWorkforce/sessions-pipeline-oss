# Canonical directory display values

Selected Supabase list and four-pool hybrid results can need exact opening text or a local project path. Directory metadata stores only the locator and fingerprint. Those values now live in the existing immutable S3 replay artifact graph, under raw table `session_directory_event_values`.

The trusted Node producer uses the same complete canonical source fold and the exact opening/project locators already computed for #171. It fetches at most two rows from the fold's identity map; it does not perform another fold or reconstruct a fingerprint from truncated content. An event serving both roles is emitted once. Opening text applies the legacy ECMAScript whitespace normalization and 500 Unicode-codepoint prefix; the bounded scanner never copies the large unused tail. RHC still applies the established 240 UTF-16-character word-boundary summary. Project paths remain exact. Each display row is bounded at 1 MiB; larger paths refuse publication without truncation. The event writer does not enable this optional display projection. The live raw read projects display values only for `table=session_directory_event_values`; raw reads of every other table, timing and turns do no display work and return the same snapshot as before.

The existing graph protocol, source generation, directory epoch/fingerprint capture, immutable exact-byte SHA references, write/read verification and publication CAS remain authoritative. The Worker uses the existing JWT-authorized `session_directory_replay_pages` claim before objects and rechecks it afterward. The existing canonical HTTP endpoint permits this additional table. Missing inventories in older indices fail closed, so source scopes must be republished before selecting display readers. An empty known inventory is an explicit zero-row view, never an inferred fallback.

No Supabase text/path columns, new RPC, migration, grant, credential or binding is added. Existing `CANONICAL_READ_ENABLED=true` and `CANONICAL_READ_MODE=pages` are required for the reviewed large-session deployment. Current RHC cutover configuration requires `LIST_BACKEND=supabase`, `SESSION_SEARCH_BACKEND=v2`, `SESSION_READ_BACKEND=canonical` and the existing provider URL. An accidentally missing canonical selection fails display hydration with 503. Default and shadow behavior is preserved.

The RHC consumer groups exact source/session pairs, permits at most four in-flight provider reads, retains the existing 2 MiB per-response bound, and separately bounds total display rows across the page at 2 MiB. Failures abort sibling reads without awaiting stream cancellation. It matches full locator identity, exact microseconds and fingerprint, then re-reads directory metadata and compares epoch, projection time and locators before returning anything.

## Offline evidence and operational gates

`scripts/proof-directory-display.ts <new-empty-directory>` uses only synthetic PGlite, the real exporter/canonical fold and local S3-compatible FS storage. Run with Node 24 and the existing proof TypeScript loader. Its 1,002-event fixture includes a 2,510,020-character opening from 2020 with original machine/session IDs and an exact private path. The actual Worker proof is:

```sh
CANONICAL_ARTIFACT_TEST_FIXTURE=<fixture> node_modules/.bin/vitest run --config vitest.directory-display.config.ts
```

It reads only the authenticated index and compact display page (4,227 bytes in the measured fixture), refuses erasure during hydration, and reads no objects for authenticated absence. This proves the synthetic component path, not production corpus publication, credentials, latency or all future field widths.

Activation still requires independent review, exact-head CI, republished current source scopes, erasure/freshness checks and measured production-shaped caller latency/field bounds. Neither old CH reconciliation-tail drainage nor the old window table is a dependency of this display path. Preserve DELIVERY_FORWARD OFF for the separately approved read cutover; future forwarding/write topology remains a distinct gate. No production changes are authorized by this draft.
