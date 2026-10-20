import type { SubmissionId } from "@earendil-works/pi-durable";
import { idFromNumber } from "../ids.ts";
import { PayloadCorrupt } from "./payloads.ts";
import { Database, type Row } from "./db.ts";
import {
  check,
  errorCode,
  epoch,
  ownerToken,
  harnessId,
  integer,
  parseSha256,
  requestId,
  sleep,
  text,
  type HarnessId,
  type RequestId,
  type Sha256,
} from "../domain.ts";
import { type SqlProvider } from "./connection.ts";
import { RequestConflict } from "../faults.ts";
import { canonical, MAX_INLINE_INPUT_BYTES } from "../input.ts";
import { type AcceptedInput, type Authority, type Received, type InboxItem } from "../authority.ts";
import { publishedPayload, type StoredPayload } from "../payload.ts";
import { issueLease, LEASE_MARGIN_MS, type Claim, type Lease } from "../lease.ts";

/** SQL expression for the database clock in Unix epoch milliseconds. */
export const DB_NOW_MS = "floor(EXTRACT(EPOCH FROM now())*1000)::bigint";

async function createNamespace(db: Database, provider: SqlProvider, ns: HarnessId): Promise<void> {
  // Existing rows are never written by ingress. New free rows are immediately eligible,
  // even when local INSERT and claim share a millisecond: strict '<' needs now-1.
  await retryConfirmedAborts(provider, async () => {
    if ((await db.query("SELECT ns FROM harness WHERE ns=$1", [ns])).length) return;

    try {
      await db.query(
        `INSERT INTO harness(ns,epoch,owner,lease_until,next_seq,next_id) VALUES($1,0,NULL,${DB_NOW_MS}-1,1,2)`,
        [ns],
      );
    } catch (error) {
      if (errorCode(error) !== UNIQUE_VIOLATION) throw error;
      if (!(await db.query("SELECT ns FROM harness WHERE ns=$1", [ns])).length) throw error;
    }
  });
}

/** PostgreSQL SQLSTATE for a unique constraint violation. */
const UNIQUE_VIOLATION = "23505";

const MAX_ATTEMPTS = 3;

const RETRY_BACKOFF_MS = 20;

/** Receipts and Pi-owned live states are observed in one SQL snapshot. */
const WORK = `
  EXISTS (SELECT 1 FROM inbox i WHERE i.ns=h.ns AND i.submission_id IS NULL)
  OR EXISTS (SELECT 1 FROM records r WHERE r.ns=h.ns AND r.type='task' AND r.status IN ('pending','running','waiting','completing'))
  OR EXISTS (SELECT 1 FROM records r WHERE r.ns=h.ns AND r.type='submission' AND r.status IN ('queued','placed'))`;

/**
 * Checks that `json` is canonical Pi input within the inline cap and that `hash` is its SHA-256.
 * The byte cap is checked before parsing.
 */
function verifyInlinePayload(
  hashValue: unknown,
  jsonValue: unknown,
): { hash: Sha256; json: string } {
  const hash = parseSha256(hashValue);
  const json = text(jsonValue);

  check(Buffer.byteLength(json) <= MAX_INLINE_INPUT_BYTES, "Inbox payload integrity");

  const reparsed = canonical(JSON.parse(json));

  check(reparsed.json === json && reparsed.sha256 === hash, "Inbox payload integrity");

  return { hash, json };
}

/** Converts a stored row to a receipt and rejects rows whose payload fails verification. */
function parseInboxRow(row: Row): InboxItem {
  const ns = harnessId(row.ns);
  const hash = parseSha256(row.payload_sha256);
  let payload: StoredPayload;

  if (row.payload_kind === "inline") {
    check(row.object_key === null && row.object_bytes === null, "Inbox payload coherence");
    const { json } = verifyInlinePayload(hash, row.json);

    payload = { kind: "inline", json };
  } else {
    check(row.payload_kind === "s3" && row.json === null, "Inbox payload coherence");
    payload = publishedPayload(ns, {
      kind: "s3",
      key: row.object_key,
      sha256: hash,
      bytes: row.object_bytes,
    });
  }

  const base = {
    ns,
    request: requestId(row.request_id),
    hash,
    payload,
    receivedAt: integer(row.received_at),
  };
  if (row.submission_id === null) return { ...base, kind: "received" };
  try {
    const id = integer(row.submission_id);
    check(id > 0, "Invalid submission ID");
    return { ...base, kind: "admitted", submission: idFromNumber<SubmissionId>(id) };
  } catch (error) {
    throw new PayloadCorrupt(`${ns}/inbox/${base.request}`, error);
  }
}

/**
 * Runs `operation` again after a provider-confirmed abort, up to `MAX_ATTEMPTS` attempts.
 * Each statement autocommits, so a confirmed abort committed nothing.
 * Any other error, including an unknown outcome, is thrown without a retry.
 */
async function retryConfirmedAborts<T>(
  provider: SqlProvider,
  operation: () => Promise<T>,
): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await operation();
    } catch (error) {
      if (!provider.isRetryableAbort(error) || attempt === MAX_ATTEMPTS) throw error;
      await sleep(RETRY_BACKOFF_MS * attempt);
    }
  }
}

/** Returns `existing` as a duplicate receipt, or throws if it stores a different input. */
function duplicateOf(existing: InboxItem, hash: Sha256): { duplicate: true; item: InboxItem } {
  if (existing.hash !== hash) throw new RequestConflict();

  return { duplicate: true, item: existing };
}

export class SqlAuthority implements Authority {
  constructor(
    private readonly db: Database,
    private readonly provider: SqlProvider,
  ) {}

  /** Strict database-clock expiry; confirmed aborts retry with one fixed owner token. */
  async claim(ns: HarnessId, remainingMs: number): Promise<Claim> {
    harnessId(ns);
    check(
      Number.isSafeInteger(remainingMs) && remainingMs > 0 && remainingMs <= 900000,
      "Invalid remaining invocation time",
    );
    const owner = ownerToken();
    return retryConfirmedAborts(this.provider, async () => {
      const rows = await this.db.query(
        `UPDATE harness SET epoch=epoch+1,owner=$2,lease_until=${DB_NOW_MS}+$3::bigint+$4::bigint
           WHERE ns=$1 AND lease_until < ${DB_NOW_MS} RETURNING epoch,lease_until`,
        [ns, owner, remainingMs, LEASE_MARGIN_MS],
      );
      if (rows[0])
        return {
          kind: "claimed",
          lease: issueLease(ns, owner, epoch(rows[0].epoch), integer(rows[0].lease_until)),
        };
      check(
        (await this.db.query("SELECT ns FROM harness WHERE ns=$1", [ns])).length === 1,
        "Unknown namespace",
      );
      return { kind: "held" };
    });
  }

  /** Join clean storage closure first. On unknown write/close failure, keep the lease until expiry. */
  async release(lease: Lease): Promise<"released" | "lost"> {
    const rows = await this.db.query(
      `UPDATE harness SET owner=NULL,lease_until=${DB_NOW_MS} WHERE ns=$1 AND epoch=$2 AND owner=$3 RETURNING ns`,
      [lease.ns, lease.epoch, lease.owner],
    );
    return rows.length === 1 ? "released" : "lost";
  }

  async received(ns: HarnessId): Promise<Received[]> {
    const rows = await this.db.query(
      "SELECT * FROM inbox WHERE ns=$1 AND submission_id IS NULL ORDER BY received_at,request_id LIMIT 8",
      [ns],
    );
    return rows.map((row) => {
      const item = parseInboxRow(row);
      check(item.kind === "received", "Inbox query inconsistency");
      return item;
    });
  }

  async markAdmitted(received: Received, id: SubmissionId): Promise<void> {
    const { ns, request, hash } = received;
    harnessId(ns);
    requestId(request);
    parseSha256(hash);
    check(typeof id === "number" && Number.isSafeInteger(id) && id > 0, "Invalid submission ID");
    await retryConfirmedAborts(this.provider, async () => {
      const prior = await this.get(ns, request);
      check(prior && prior.hash === hash, "Inbox identity changed");
      if (prior.kind === "admitted") {
        check(prior.submission === id, "Admission mapping changed");
        return;
      }

      // Mapping records a completed publication, not lease ownership. A takeover must not
      // revoke the original publisher's ability to map an already-stored submission.
      const rows = await this.db.query(
        `UPDATE inbox SET submission_id=$3
         WHERE ns=$1 AND request_id=$2 AND submission_id IS NULL AND payload_sha256=$4
         AND EXISTS(SELECT 1 FROM records WHERE ns=$1 AND id=$3 AND type='submission')
         RETURNING submission_id`,
        [ns, request, id, hash],
      );
      if (!rows.length) {
        const current = await this.get(ns, request);
        check(
          current?.kind === "admitted" && current.submission === id && current.hash === hash,
          "Submission missing or admission mapping changed",
        );
      }
    });
  }

  async hasWork(ns: HarnessId): Promise<boolean> {
    const rows = await this.db.query(`SELECT (${WORK}) AS work FROM harness h WHERE h.ns=$1`, [ns]);
    check(rows.length === 1 && typeof rows[0].work === "boolean", "Unknown namespace/work result");
    return rows[0].work;
  }

  close(): Promise<void> {
    return this.db.end();
  }

  async get(ns: HarnessId, request: RequestId): Promise<InboxItem | undefined> {
    const rows = await this.db.query("SELECT * FROM inbox WHERE ns=$1 AND request_id=$2", [
      ns,
      request,
    ]);

    return rows[0] ? parseInboxRow(rows[0]) : undefined;
  }

  async receive(
    ns: HarnessId,
    request: RequestId,
    accepted: AcceptedInput,
  ): Promise<{ duplicate: boolean; item: InboxItem }> {
    // Read each caller-owned field once, before the first await, then validate those values.
    const hash = parseSha256(accepted.hash);
    const source = accepted.payload;
    const kind = source.kind;
    let payload: StoredPayload;

    // Branded IDs can be forged with a cast, so validate them before any SQL.
    harnessId(ns);
    requestId(request);

    if (kind === "inline") {
      const { json } = verifyInlinePayload(hash, source.json);

      payload = { kind, json };
    } else {
      check(kind === "s3", "Inbox payload coherence");
      payload = publishedPayload(ns, {
        kind,
        key: source.key,
        sha256: source.sha256,
        bytes: source.bytes,
      });
      check(payload.sha256 === hash, "Inbox payload integrity");
    }

    await createNamespace(this.db, this.provider, ns);
    // Each receipt attempt re-reads the winner; namespace retries are a separate bounded stage.
    return retryConfirmedAborts(this.provider, async () => {
      const existing = await this.get(ns, request);

      if (existing) return duplicateOf(existing, hash);

      try {
        const rows = await this.db.query(
          `INSERT INTO inbox(ns,request_id,payload_sha256,payload_kind,json,object_key,object_bytes,received_at)
           VALUES($1,$2,$3,$4,$5,$6,$7,${DB_NOW_MS}) RETURNING *`,
          [
            ns,
            request,
            hash,
            payload.kind,
            payload.kind === "inline" ? payload.json : null,
            payload.kind === "s3" ? payload.key : null,
            payload.kind === "s3" ? payload.bytes : null,
          ],
        );

        check(rows[0], "Inbox insert missing result");

        return { duplicate: false, item: parseInboxRow(rows[0]) };
      } catch (error) {
        if (errorCode(error) !== UNIQUE_VIOLATION) throw error;

        // A concurrent request stored first. Its receipt decides the outcome.
        const winner = await this.get(ns, request);

        if (!winner) throw error;

        return duplicateOf(winner, hash);
      }
    });
  }
}
