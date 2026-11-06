# Canonical derived-read protocol additions

The optional canonical provider accepts an expected `generation` on any table request. It verifies the same current source set before returning data; mismatch returns503/source_generation_changed. History must also compare every response generation so an incorrectly configured provider cannot mix snapshots.

`view=bounds` applies only to convergence_events and returns identity/time fields without transcript or tool payload. It supports exact session bounds without transporting large content. `view=legacy` applies only to the three scoped legacy tables and preserves every payload field, with the same64MiB first-row ceiling as replay; remaining pages target2MiB. View/table/generation remain bound into the existing cursor. Content limits apply only to replay.

Responses attest `exists`: false only when the authenticated pointer RPC returned no sources, with no rows/coverage/cursor. Existing source objects without explicit legacy coverage remain unknown. This differentiates a missing canonical source from an existing export whose legacy tables were never scanned.

These additions support the History thread adapter and future derived readers. No flag, auth role, schema, grant, process, credential or deployment is changed. Source16MiB/100k bounds and legacy future-writer fencing remain activation blockers; this protocol does not claim large-session capacity proof or complete public API migration.
