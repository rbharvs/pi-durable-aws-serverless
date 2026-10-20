import { randomUUID } from "node:crypto";
import type { Row, SqlValue } from "./db.ts";
import { check, computeSha256, text, type HarnessId } from "../domain.ts";
import { uploadValues } from "../object-data.ts";
import type { PublishedPayload as S3Payload } from "../payload.ts";
import { jsonText, type StoredPayload } from "../payload.ts";
import { LIMITS } from "../limits.ts";
import { objectSize, reference, referenceEstimate, type PayloadObjects } from "../s3-payload.ts";

type PayloadRef = string & { readonly __payloadRef: unique symbol };

function payloadRef(value: unknown): PayloadRef {
  const ref = text(value);

  check(
    /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(ref),
    "Invalid payload reference",
  );

  return ref as PayloadRef;
}

type PayloadMetadata = {
  readonly ref: PayloadRef;
  readonly payload: { readonly kind: "inline" } | S3Payload;
};

/** Ownership and envelope checks need no inline body, including when replacing a record. */
function storedMetadata(row: Row, ns: HarnessId): PayloadMetadata {
  check(row.ns === ns, "Foreign owner namespace");

  const ref = payloadRef(row.payload_ref);

  if (row.payload_kind === "inline") {
    check(
      row.inline_ref === ref && row.s3_ref == null,
      "Inline payload missing, wrong kind or both present",
    );
    check(
      row.object_key == null && row.sha256 == null && row.bytes == null,
      "Unexpected S3 columns",
    );

    return { ref, payload: { kind: "inline" } };
  }

  check(row.payload_kind === "s3", "Invalid payload kind");
  check(
    row.s3_ref === ref && row.inline_ref == null && row.inline_json == null,
    "S3 payload missing, wrong kind or both present",
  );

  return { ref, payload: reference(row, ns) };
}

const INSERT_INLINE = "INSERT INTO inline_payloads(ns,payload_ref,json) VALUES($1,$2,$3)";

const INSERT_S3 =
  "INSERT INTO s3_payloads(ns,payload_ref,object_key,sha256,bytes) VALUES($1,$2,$3,$4,$5)";

const DELETE_INLINE = "DELETE FROM inline_payloads WHERE ns=$1 AND payload_ref=$2";

const DELETE_S3 = "DELETE FROM s3_payloads WHERE ns=$1 AND payload_ref=$2";

type Selected = {
  readonly input: PayloadInput;
  readonly ref: PayloadRef;
  readonly size: number;
  previous?: { readonly ref: PayloadRef; readonly kind: StoredPayload["kind"] };
  kind: StoredPayload["kind"];
};

function deletion(value: Selected): readonly Statement[] {
  return value.previous
    ? [
        {
          sql: value.previous.kind === "inline" ? DELETE_INLINE : DELETE_S3,
          params: [value.input.ns, value.previous.ref],
        },
      ]
    : [];
}

/** JSON bytes count once. Scalar metadata and extra SQL/mutations count separately. */
function estimate(values: readonly Selected[], metadataBytes: number): number {
  return values.reduce((total, v) => {
    const scalar =
      v.kind === "inline"
        ? [v.input.ns, v.ref]
        : [v.input.ns, v.ref, ...referenceEstimate(v.input.ns, v.size)];
    const insert = v.kind === "inline" ? INSERT_INLINE : INSERT_S3;
    const removed = deletion(v);

    return (
      total +
      3 * Buffer.byteLength(JSON.stringify([v.kind, v.ref])) +
      Buffer.byteLength(insert) +
      3 * Buffer.byteLength(JSON.stringify(scalar)) +
      2048 +
      (v.kind === "inline" ? v.size : 0) +
      removed.reduce(
        (n, s) =>
          n + Buffer.byteLength(s.sql) + 3 * Buffer.byteLength(JSON.stringify(s.params)) + 2048,
        0,
      )
    );
  }, metadataBytes);
}

export type OwnerTable = "records" | "revisions";

export type PayloadInput = {
  readonly ns: HarnessId;
  readonly id: number;
  readonly json: string;
} & (
  | {
      readonly table: "records";
      /** Existing owner, read in validation and protected by the next_seq fence. */
      readonly previous?: Row;
    }
  | {
      readonly table: "revisions";
      readonly previous?: never; // revisions are insert-only
    }
);

export type Statement = {
  readonly sql: string;
  readonly params: readonly SqlValue[];
};

export type PreparedPayload = {
  readonly params: readonly SqlValue[];
  readonly before: readonly Statement[];
  readonly after: readonly Statement[];
};

export type PayloadPlan = {
  /** Includes owner rows, payload insert/deletes, not the storage fence. */
  readonly rowMutations: number;
  readonly estimatedBytes: number;
  /** Called only AFTER the complete row/byte envelope passes. No SQL here. */
  upload(): Promise<readonly PreparedPayload[]>;
};

export class PayloadCorrupt extends Error {
  constructor(key: string, cause: unknown) {
    super(`PayloadCorrupt: ${key}`, { cause });
  }
}

export const PAYLOAD_COLUMNS = ["payload_kind", "payload_ref"] as const;

// Probe both payload tables regardless of kind, without fetching the previous inline body.
export const referenceProjection = (table: OwnerTable): string =>
  `${table}.*,
       (SELECT ip.payload_ref FROM inline_payloads ip WHERE ip.ns=${table}.ns AND ip.payload_ref=${table}.payload_ref) AS inline_ref,
       (SELECT sp.payload_ref FROM s3_payloads sp WHERE sp.ns=${table}.ns AND sp.payload_ref=${table}.payload_ref) AS s3_ref,
       (SELECT sp.object_key FROM s3_payloads sp WHERE sp.ns=${table}.ns AND sp.payload_ref=${table}.payload_ref) AS object_key,
       (SELECT sp.sha256 FROM s3_payloads sp WHERE sp.ns=${table}.ns AND sp.payload_ref=${table}.payload_ref) AS sha256,
       (SELECT sp.bytes FROM s3_payloads sp WHERE sp.ns=${table}.ns AND sp.payload_ref=${table}.payload_ref) AS bytes`;

/** Reads add the inline body only for owners selected by the query. */
export const projection = (table: OwnerTable): string =>
  `${referenceProjection(table)},
       (SELECT ip.json FROM inline_payloads ip WHERE ip.ns=${table}.ns AND ip.payload_ref=${table}.payload_ref) AS inline_json`;

/** Fixed hybrid payload representation: complete planning precedes every object upload. */
export class Payloads {
  constructor(
    private readonly ns: HarnessId,
    private readonly objects: PayloadObjects,
  ) {}

  plan(values: readonly PayloadInput[], metadataBytes: number): PayloadPlan {
    const ns = this.ns,
      objects = this.objects;
    const earlier = new Map<string, Selected>();
    const selected: Selected[] = values.map((source) => {
      const input = { ...source };
      check(input.ns === ns, "Foreign payload namespace");

      const size = objectSize(input.json);
      let old: PayloadMetadata | undefined;

      if (input.previous) {
        try {
          old = storedMetadata(input.previous, ns);
        } catch (error) {
          throw new PayloadCorrupt(`${ns}/${input.table}/${input.id}`, error);
        }
      }

      const owner = `${input.table}/${input.id}`;
      const prior = input.table === "records" ? earlier.get(owner) : undefined;
      const kind = size > LIMITS.maxInlineBytes ? "s3" : "inline";
      const value: Selected = {
        input,
        size,
        ref: payloadRef(randomUUID()),
        kind,
        previous: prior
          ? { ref: prior.ref, kind: prior.kind }
          : old
            ? { ref: old.ref, kind: old.payload.kind }
            : undefined,
      };

      if (input.table === "records") earlier.set(owner, value);

      return value;
    });

    function linkIntermediateDeletes(): void {
      earlier.clear();

      for (const v of selected) {
        if (v.input.table !== "records") continue;

        const owner = `records/${v.input.id}`,
          prior = earlier.get(owner);

        if (prior) v.previous = { ref: prior.ref, kind: prior.kind };
        earlier.set(owner, v);
      }
    }

    for (const v of [...selected].sort((a, b) => b.size - a.size)) {
      linkIntermediateDeletes();
      if (estimate(selected, metadataBytes) <= LIMITS.maxTransactionBytes) break;

      if (v.kind === "inline") {
        v.kind = "s3";
      }
    }

    linkIntermediateDeletes();

    const extraDeletes = selected.filter((v) => v.previous).length;

    return {
      rowMutations: 2 * selected.length + extraDeletes,
      estimatedBytes: estimate(selected, metadataBytes),

      async upload() {
        const uploads = selected.filter((v) => v.kind === "s3");
        const refs = await uploadValues(uploads, async (v) => {
          const result = await objects.put(v.input.json);
          check(result.kind === "s3", "Invalid uploaded payload kind");
          const ref = reference(
            { object_key: result.key, sha256: result.sha256, bytes: result.bytes },
            ns,
          );
          check(
            ref.bytes === v.size && ref.sha256 === computeSha256(v.input.json),
            "Uploaded payload integrity mismatch",
          );
          return ref;
        });
        const uploaded = new Map<PayloadRef, S3Payload>(uploads.map((v, i) => [v.ref, refs[i]!]));

        return selected.map((v) => {
          let before: Statement;

          if (v.kind === "inline")
            before = {
              sql: INSERT_INLINE,
              params: [ns, v.ref, v.input.json],
            };
          else {
            const ref = uploaded.get(v.ref);

            check(ref, "Missing prepared S3 reference");
            before = {
              sql: INSERT_S3,
              params: [ns, v.ref, ref.key, ref.sha256, ref.bytes],
            };
          }

          return {
            params: [v.kind, v.ref],
            before: [before],
            after: deletion(v),
          };
        });
      },
    };
  }

  async decode(row: Row, key: string): Promise<string> {
    const ns = this.ns,
      objects = this.objects;

    try {
      const { payload } = storedMetadata(row, ns);

      if (payload.kind === "inline") {
        const json = jsonText(row.inline_json);
        check(Buffer.byteLength(json) <= LIMITS.maxInlineBytes, "Inline payload exceeds bound");
        return json;
      }
      const expectedBytes = payload.bytes,
        expectedHash = payload.sha256;
      const json = jsonText(await objects.get(payload));
      check(
        Buffer.byteLength(json) === expectedBytes && computeSha256(json) === expectedHash,
        "Object payload integrity mismatch",
      );
      return json;
    } catch (error) {
      throw new PayloadCorrupt(key, error);
    }
  }
}
