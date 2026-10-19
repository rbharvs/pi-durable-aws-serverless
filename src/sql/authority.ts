import { Database, type Row } from "./db.ts";
import {
  check,
  digest,
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

export async function createNamespace(
  db: Database,
  provider: SqlProvider,
  ns: HarnessId,
): Promise<void> {
  // Existing rows are never written by ingress. New free rows are immediately eligible,
  // even when local INSERT and claim share a millisecond: strict '<' needs now-1.
  for (let attempt = 1; ; attempt++) {
    if ((await db.query("SELECT ns FROM harness WHERE ns=$1", [ns])).length) return;

    try {
      await db.query(
        `INSERT INTO harness(ns,epoch,owner,lease_until,next_seq,next_id) VALUES($1,0,NULL,${DB_NOW_MS}-1,1,2)`,
        [ns],
      );

      return;
    } catch (error) {
      const duplicate =
        typeof error === "object" && error !== null && "code" in error && error.code === "23505";

      if (!duplicate && !provider.isRetryableAbort(error)) throw error;
      if ((await db.query("SELECT ns FROM harness WHERE ns=$1", [ns])).length) return;
      if (duplicate || attempt === 3) throw error;
      await sleep(20 * attempt);
    }
  }
}

function item(row: Row): Received {
  const hash = digest(row.payload_sha256);

  check(row.payload_kind === "inline", "Inbox payload coherence");

  const json = text(row.json),
    c = canonical(JSON.parse(json));

  check(
    row.object_key === null &&
      row.object_bytes === null &&
      c.bytes <= MAX_INLINE_INPUT_BYTES &&
      c.json === json &&
      c.sha256 === hash,
    "Inbox payload integrity",
  );
  // A mapped receipt cannot be represented as received.
  check(row.submission_id === null, "Unsupported inbox admission mapping");

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
    // Detach before the first await.
    const { hash, payload } = structuredClone(accepted);

    // Validate coherence before insertion as well as on every read.
    item({
      ns,
      request_id: request,
      payload_sha256: hash,
      payload_kind: payload.kind,
      json: payload.json,
      object_key: null,
      object_bytes: null,
      received_at: 0,
      submission_id: null,
    });
    await createNamespace(this.db, this.provider, ns);

    return repeatable(this.provider, async () => {
      const existing = await this.get(ns, request);

      if (existing) {
        if (existing.hash !== hash) throw new RequestConflict();

        return { duplicate: true, item: existing };
      }

      try {
        const rows = await this.db.query(
          `INSERT INTO inbox(ns,request_id,payload_sha256,payload_kind,json,received_at,submission_id)
           VALUES($1,$2,$3,$4,$5,${DB_NOW_MS},NULL) RETURNING *`,
          [ns, request, hash, payload.kind, payload.json],
        );

        check(rows[0], "Inbox insert missing result");

        return { duplicate: false, item: item(rows[0]) };
      } catch (e) {
        if (!(typeof e === "object" && e !== null && "code" in e && e.code === "23505")) throw e;

        const winner = await this.get(ns, request);

        if (!winner || winner.hash !== hash) throw new RequestConflict();

        return { duplicate: true, item: winner };
      }
    });
  }
}
