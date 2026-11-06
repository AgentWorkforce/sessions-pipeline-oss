# Dormant bounded canonical page consumer

The canonical Worker route retains its default disabled behavior. With existing `CANONICAL_READ_ENABLED=true`, `CANONICAL_READ_MODE=pages` selects the v1 immutable page protocol. Omitted mode preserves the prior raw reader; an unknown mode refuses. These are implementation switches, not authorization to change production configuration.

Selected pages mode calls `sessions.session_directory_replay_pages` with the SAME incoming worker bearer and exact workspace/harness/session key before reading any object. It never invokes the raw-record projector or a Neon client, and never falls back on absent, unpublished, stale, malformed or failed responses. An authenticated absent key returns an empty result only after a matching absence-fingerprint recheck. Live unpublished/stale state returns typed503.

Dependencies: the Node producer/publication change must install the capture/CAS/publication RPC with its claim-bound ACL, materialize the current source generation, and publish its verified index. The current raw object-store bindings and canonical Supabase gateway bindings remain required. Existing RHC bridge token exchange and tenant assertions remain in force. No direct session_index/table grants or caller-provided bucket/key exist.

The consumer hashes exact bytes before parsing each object; accepts only keys within the RPC-attested prefix; checks identity/generation/source fingerprint+epoch, range continuity, tree depth/fanout, table/view and final publication currentness. It fetches at most256KiB objects/64KiB segments, four segment requests concurrently and4096 objects total. Ordinary responses target2MiB; the preexisting first-replay/legacy-row ceiling is64MiB. That ceiling is a refusal boundary, not a claim that arbitrary64MiB rows fit Worker memory. Capacity proof records the actual large row exercised.

`raw`, `replay`, `bounds` and legacy-table views preserve the existing provider contract. Replay native-message identity and every mandatory flat field/tool array remain present; only duplicate raw payload is omitted by the shared producer view. Large replay content is separately JSON-string-segmented losslessly, including lone surrogates. `maxContent=N` reads only the necessary prefix and returns at mostN+1 UTF16 units, matching the existing public truncation mapper. Large mandatory arrays/objects use out-of-band segmented row JSON and are fully reconstructed. No sentinel inside user payload is interpreted as a codec reference.

The backend cursor binds exact identity, source generation, index SHA, table, view, order, maxContent and ordinal. Source mutation or a different publication invalidates it. Event ordering uses the shared producer comparator, and each hydrated page checks returned adjacent rows. The8s Worker deadline cancels pending transport; RHC's existing12s outer budget remains.

Raw-mode cursors and `generation` values are not portable to pages mode. The two modes use different generation semantics, and raw cursors are plain base64 while pages cursors carry the `artifact-v1:` prefix. After a `CANONICAL_READ_MODE` flip from raw to pages, a caller still holding raw-mode state gets `source_generation_changed`/503 for a stale `generation` or `cursor`/400 for a raw cursor; it never receives rows resumed from the wrong position. This is fail-closed by design, but activation must plan for it: callers must restart pagination from the first page and drop cached raw generations when the mode changes.

`table=session_timing` in pages mode answers `501 unsupported_by_backend` with reason `timing_turns_unknown` (the timing route's refusal, not a retryable `artifact_view`/503) for an index with `legacyCoverage: null` and no `session_timing` table, until the producer publishes legacy turn coverage for that session.

## Offline verification

Run the bounded synthetic graph and real workerd routing tests with `bun test src/session-read-api/__tests__/canonical-artifact-reader.test.ts` and package `test:canonical`. Removing object digest verification makes the prefix-content corruption control fail; disabling selected pages dispatch makes the workerd positive routing control fail.

The optional package `test:artifact-capacity` uses `CANONICAL_ARTIFACT_TEST_FIXTURE` pointing to the Node producer's synthetic `objects/` and `claim.json` output. A Node-only loopback server streams individual files; workerd never imports the corpus into a Map. The proof checks first/next/final pages of at least300001 real materialized events, object/fetch/byte bounds, full large replay plus mandatory tool arrays/native IDs, and prefix semantics. This is offline test configuration, not a production object path. It is intentionally separate from ordinary CI discovery so an absent local large fixture cannot silently skip or break ordinary tests.

No activation readiness claim is made by this patch. Actual applied SQL/ACL, complete backfill/publication, current fingerprints/epochs, production capacity and every audited Desktop/MCP route/search destination remain release gates. The accepted caller inventory is maintained in the coordinating run's desktop-read-caller-audit.md. Thread/full unrelated enrichment parity is outside that accepted scope.

## Recorded actual capacity proof

Producer `7623abf867e4c30254a2facfcd46b009da653cd1` materialized and CAS-published300001 real synthetic canonical events from187578412 raw bytes across2manifest sources. Node24.21.0 took80.6s/peakRSS2397769728bytes, producing3655objects/927101388artifact bytes; root index3105bytes. This is an observed fixture, not proof of every256MiB/1Mrecord budget extreme.

The actual workerd consumer proof passes3cases: first/next/final ordinal pages;7exact producer-oracle positions including physical page boundaries/middle/end and native IDs/counters; full2360000-character content plus5000mandatorytoolcalls atordinal1800, with maxContent80 preserving allmandatoryfields. The inclusive tied timestamp window takes16object reads/2804610bytes, largestobject262126bytes. The Worker does not load the full corpus. Ordinary timestamp windows use binary ordinal seek through the separately hashed bounds view; since/until are bound into the cursor. The public RHC adapter retains its existing parsed-date precision at its boundary.

## Repaired producer composition proof

The consumer is restacked on producer d89d3e53d6a02a24d537638c85248b75d8d04fea, which requires live directory authority and closes the independent directory hard-delete publication finding. The immutable v2b synthetic fixture at replay-pages-20261103-capacity-v2b includes300001 actual CH-codec rows and171metadata ready/dirty0 before current epoch5 publication. Eight ordinary workerd tests and four large-corpus proofs pass against the new claim, including the actual HTTP middle-window path and changed-window cursor refusal. Two HTTP regressions fail against old3b and pass the forwarding repair.

The combined Node proof peaks at3181887488bytes including PGlite and its recorder and takes46.85s. It does not establish current3008MiB Lambda fit. Production metadata identifies21canonical cohorts above256MiB (largest~1.53GB), so actual large-source Node publication capacity remains a release blocker. No Worker source-memory limit is raised and no activation readiness is claimed.
