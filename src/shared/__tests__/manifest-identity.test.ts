import { describe, expect, test } from "bun:test";
import type { ManifestV1 } from "../vendor.ts";
import {
  canonicalManifestIdentity,
  manifestUserId,
} from "../manifest-identity.ts";

const manifest = (extensions: Record<string, unknown>): ManifestV1 =>
  ({
    session: {
      session_id: "kimi_session",
      machine_id: "delivery_abc",
    },
    extensions,
  }) as ManifestV1;

describe("manifest identity", () => {
  test("restores source ids only when they encode to the physical layout ids", () => {
    expect(
      canonicalManifestIdentity(
        manifest({
          "96bulls": {
            canonical_session_id: "kimi:session",
            canonical_machine_id: "delivery:abc",
          },
        }),
      ),
    ).toEqual({ sessionId: "kimi:session", machineId: "delivery:abc" });
    expect(
      canonicalManifestIdentity(
        manifest({
          "96bulls": {
            canonical_session_id: "another:session",
            canonical_machine_id: "another:machine",
          },
        }),
      ),
    ).toEqual({ sessionId: "kimi_session", machineId: "delivery_abc" });
  });

  test("ignores an exporter marker without its admitted provenance", () => {
    expect(
      canonicalManifestIdentity(
        manifest({
          "client.relayhistory_export": {
            catalog: { neon_session_id: "kimi:session" },
          },
        }),
      ),
    ).toEqual({ sessionId: "kimi_session", machineId: "delivery_abc" });
  });

  test("refuses an export user that is not bound by the marker", () => {
    expect(
      manifestUserId(
        manifest({
          "96bulls": { user_id: "user-a" },
          "client.relayhistory_export": { source_user_id: "user-b" },
        }),
      ),
    ).toBeUndefined();
    expect(manifestUserId(manifest({ "96bulls": { user_id: "user-a" } }))).toBe(
      "user-a",
    );
  });
});
