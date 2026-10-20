import { Readable } from "node:stream";
import { objectSize, reference, type PayloadObjects } from "../../src/s3-payload.ts";
import { check, computeSha256, type HarnessId } from "../../src/domain.ts";
import {
  stagedReference,
  type StagedReference,
  ObjectInvalid,
  type CommandObjects,
} from "../../src/index.ts";

/**
 * Deterministic adapter contract fixture, NOT an S3 emulator or compatibility test.
 * Mutable staging and create-only published bytes have separate private ownership.
 * Inspection returns copies. Runtime handles cannot upload staging or overwrite publications.
 */
export function objectFixture() {
  const staging = new Map<string, Buffer>();
  const published = new Map<string, Buffer>();
  let sequence = 0;
  let openHandles = 0;

  function name(): string {
    sequence++;

    return `00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`;
  }

  return {
    stage(ns: HarnessId, value: string | Uint8Array, keyName = "input"): StagedReference {
      const bytes = Buffer.from(value);
      const ref = stagedReference(ns, {
        key: `in/${ns}/${keyName}`,
        sha256: computeSha256(bytes),
        bytes: bytes.length,
      });

      staging.set(ref.key, bytes);

      return ref;
    },

    deleteStage(key: string): void {
      staging.delete(key);
    },

    publishedKeys(): string[] {
      return [...published.keys()];
    },

    inspectPublished(key: string): Buffer | undefined {
      const bytes = published.get(key);

      return bytes && Buffer.from(bytes);
    },

    openHandles: () => openHandles,

    objects(ns: HarnessId): CommandObjects & PayloadObjects {
      let closed = false;

      openHandles++;

      return {
        async get(input) {
          check(!closed, "Objects are closed");
          const ref = reference(
            { object_key: input.key, sha256: input.sha256, bytes: input.bytes },
            ns,
          );
          const bytes = published.get(ref.key);
          check(
            bytes && bytes.length === ref.bytes && computeSha256(bytes) === ref.sha256,
            "Published object missing or corrupt",
          );
          return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes);
        },

        async getStaged(input) {
          check(!closed, "Objects are closed");
          const ref = stagedReference(ns, input);
          const bytes = staging.get(ref.key);

          if (!bytes) throw new ObjectInvalid(new Error("Missing staging object"));

          return Readable.from([Buffer.from(bytes)]);
        },

        async put(json) {
          check(!closed, "Objects are closed");
          const bytes = Buffer.from(json);
          const ref = reference(
            {
              object_key: `p/${ns}/${name()}`,
              sha256: computeSha256(bytes),
              bytes: objectSize(json),
            },
            ns,
          );

          check(!published.has(ref.key), "Create-only publication collision");
          published.set(ref.key, bytes);

          return ref;
        },

        close() {
          check(!closed, "Objects already closed");
          closed = true;
          openHandles--;
        },
      };
    },
  };
}
