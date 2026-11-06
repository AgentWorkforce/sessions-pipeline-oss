/**
 * Recover the source identity that had to be encoded for the object-layout path.
 *
 * Object keys accept `[A-Za-z0-9._-]`, while RelayHistory session and machine ids
 * can contain `:` and other characters. Writers therefore keep the original ids in
 * server-admitted manifest metadata. We accept an original only when applying the
 * one shared layout encoding produces the manifest's physical id; malformed or
 * unrelated metadata can never redirect a pointer to another object-layout key.
 */
import { relayhistoryExportProvenance } from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import type { ManifestV1 } from "../../packages/object-store/src/index.ts";
import { layoutComponent } from "./layout-component.ts";

const MAX_ID_CODE_POINTS = 255;

function sourceId(value: unknown, physical: string): string | undefined {
  if (typeof value !== "string" || value.length === 0)
    return undefined;
  if (Array.from(value).length > MAX_ID_CODE_POINTS || /[\u0000-\u001f\u007f]/.test(value))
    return undefined;
  return layoutComponent(value) === physical ? value : undefined;
}

export type CanonicalManifestIdentity = {
  sessionId: string;
  machineId: string;
};

export function canonicalManifestIdentity(
  manifest: ManifestV1,
): CanonicalManifestIdentity {
  const physicalSession = manifest.session.session_id;
  const physicalMachine = manifest.session.machine_id;
  const bulls = manifest.extensions?.["96bulls"] as
    | Record<string, unknown>
    | undefined;
  const marker = manifest.extensions?.["client.relayhistory_export"] as
    | { catalog?: Record<string, unknown> }
    | undefined;
  let catalog: Record<string, unknown> | undefined;
  if (marker !== undefined) {
    try {
      relayhistoryExportProvenance(manifest);
      catalog = marker.catalog;
    } catch {
      // A marker without the signed/admitted export binding is untrusted.
    }
  }

  return {
    sessionId:
      sourceId(bulls?.canonical_session_id, physicalSession) ??
      sourceId(catalog?.neon_session_id, physicalSession) ??
      physicalSession,
    machineId:
      sourceId(bulls?.canonical_machine_id, physicalMachine) ??
      sourceId(catalog?.neon_machine_id, physicalMachine) ??
      physicalMachine,
  };
}

/**
 * The user carried by an admitted manifest. Migration exports bind it twice
 * (the export marker and `96bulls` extension); live Courier manifests bind it to
 * the authenticated upload before the message is enqueued. Callers still prefer
 * the request identity and an already-recorded server-side identity.
 */
export function manifestUserId(manifest: ManifestV1): string | undefined {
  const bulls = manifest.extensions?.["96bulls"] as
    | Record<string, unknown>
    | undefined;
  const value = bulls?.user_id;
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    Array.from(value).length > MAX_ID_CODE_POINTS ||
    /[\u0000-\u001f\u007f]/.test(value)
  )
    return undefined;

  const marker = manifest.extensions?.["client.relayhistory_export"];
  if (marker !== undefined) {
    try {
      if (relayhistoryExportProvenance(manifest).sourceUserId !== value)
        return undefined;
    } catch {
      return undefined;
    }
  }
  return value;
}
