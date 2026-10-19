import { Database, type Row } from "./db.ts";
import {
  check,
  digest,
  errorCode,
  harnessId,
  integer,
  requestId,
  sleep,
  text,
  type HarnessId,
  type RequestId,
} from "../domain.ts";
import { type SqlProvider } from "./connection.ts";
import { RequestConflict } from "../faults.ts";
import { type AcceptedInput } from "../submit.ts";
import { canonical, MAX_INLINE_INPUT_BYTES } from "../input.ts";
import { type Authority, type Received } from "../authority.ts";

export const DB_NOW_MS = "floor(EXTRACT(EPOCH FROM now())*1000)::bigint";

function item(row: Row): Received {
  const hash = digest(row.payload_sha256);

  const json = text(row.json);

  check(Buffer.byteLength(json) <= MAX_INLINE_INPUT_BYTES, "Inbox payload integrity");

  const c = canonical(JSON.parse(json));

  check(c.json === json && c.sha256 === hash, "Inbox payload integrity");

  return {
    kind: "received",
    ns: harnessId(row.ns),
    request: requestId(row.request_id),
    hash,
    payload: { kind: "inline", json },
    receivedAt: integer(row.received_at),
  };
}

/** These are single autocommit statements. A provider-confirmed abort committed nothing.
 * Re-entering the operation first re-reads authority; unknown outcomes are never retried. */
async function repeatable<T>(provider: SqlProvider, operation: () => Promise<T>): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await operation();
    } catch (e) {
      if (!provider.isRetryableAbort(e) || attempt === 3) throw e;
      await sleep(20 * attempt);
    }
  }
}

export class SqlAuthority implements Authority {
  constructor(
    private readonly db: Database,
    private readonly provider: SqlProvider,
  ) {}

  close(): Promise<void> {
    return this.db.end();
  }

  async get(ns: HarnessId, request: RequestId): Promise<Received | undefined> {
    const rows = await this.db.query("SELECT * FROM inbox WHERE ns=$1 AND request_id=$2", [
      ns,
      request,
    ]);

    return rows[0] ? item(rows[0]) : undefined;
  }

  async receive(
    ns: HarnessId,
    request: RequestId,
    accepted: AcceptedInput,
  ): Promise<{ duplicate: boolean; item: Received }> {
    // Snapshot only the scalar receipt fields before the first await, then validate them.
    const hash = accepted.hash,
      payload = accepted.payload,
      kind = payload.kind,
      json = payload.json;

    check(kind === "inline", "Inbox payload coherence");
    item({ ns, request_id: request, payload_sha256: hash, json, received_at: 0 });

    return repeatable(this.provider, async () => {
      let existing = await this.get(ns, request);

      if (!existing) {
        try {
          const rows = await this.db.query(
            `INSERT INTO inbox(ns,request_id,payload_sha256,json,received_at)
             VALUES($1,$2,$3,$4,${DB_NOW_MS}) RETURNING *`,
            [ns, request, hash, json],
          );

          check(rows[0], "Inbox insert missing result");

          return { duplicate: false, item: item(rows[0]) };
        } catch (error) {
          if (errorCode(error) !== "23505") throw error;
          existing = await this.get(ns, request);
          if (!existing) throw error;
        }
      }

      if (existing.hash !== hash) throw new RequestConflict();

      return { duplicate: true, item: existing };
    });
  }
}
