import type { Context } from "@earendil-works/chord";
import { StorageRequestError } from "@earendil-works/pi-durable";
import { idFromNumber, seqFromNumber } from "../ids.ts";
import { scanStart, nextCursor } from "./scan.ts";
import type {
  Seq,
  Id,
  ConversationId,
  ConversationRecord,
  ConversationQuery,
  EntryId,
  EntryRecord,
  EntryQuery,
  Cursor,
  Page,
  ScanOrder,
} from "@earendil-works/pi-durable";
import { BEGIN_WRITE, BEGIN_READ, Database, type Row, type SqlValue } from "./db.ts";
import type { SqlProvider } from "./connection.ts";
import {
  Payloads,
  projection,
  PAYLOAD_COLUMNS,
  PayloadCorrupt,
  type PayloadInput,
  type PayloadPlan,
} from "./payloads.ts";
import { assertEnvelope } from "../payload.ts";
import type { PayloadObjects } from "../s3-payload.ts";
import type { Lease } from "../lease.ts";
import { freezeWrites as freeze, storedHighWater, type HistoryWrite } from "../write-bounds.ts";
import { validateRecord, type RecordType, type Records } from "./records.ts";
import { check, integer, computeSha256, Mutex, sleep } from "../domain.ts";

type Mutation = { sql: string; params: SqlValue[]; payload: PayloadInput };

type AdapterState = "open" | "failed" | "closed";

/** Selectors are safe numbers, including empty/negative bounds; never coerce decimal strings. */
function assertSafeInteger(value: unknown): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value))
    throw new StorageRequestError("Expected safe integer number");
}

function assertId(value: unknown): asserts value is number {
  assertSafeInteger(value);
  if (value <= 0) throw new StorageRequestError("Expected positive ID");
}

/** Persisted head/fork points follow the decoded envelope's nonnegative numeric range. */
function assertPoint(value: unknown): asserts value is number {
  assertSafeInteger(value);
  if (value < 0) throw new StorageRequestError("Expected nonnegative history point");
}

/** Only allocation/sequence heads may contain the exact exhausted sentinel. */
function headCounter(value: unknown): number {
  const exhausted = Number.MAX_SAFE_INTEGER + 1;
  return value === exhausted || value === String(exhausted) ? exhausted : integer(value);
}

function page<T extends { id: Id<string> }>(
  rows: T[],
  limit: number,
  order: ScanOrder,
): Page<T, Cursor> {
  const items = rows.slice(0, limit);

  return rows.length > limit ? { items, next: nextCursor(items.at(-1)!.id, order) } : { items };
}

/** Native shared SQL. All connection use is private and serialized by one FIFO gate. */
export class SqlStorage {
  private readonly gate = new Mutex();
  private state: AdapterState = "open";
  private readonly payloads: Payloads;

  get status(): AdapterState {
    return this.state;
  }

  private closing: Promise<void> | undefined;
  private nextSeq: number;

  private constructor(
    private readonly db: Database,
    private readonly lease: Lease,
    private readonly provider: SqlProvider,
    objects: PayloadObjects,
    nextSeq: number,
  ) {
    this.nextSeq = nextSeq;
    this.payloads = new Payloads(lease.ns, objects);
  }

  static async open(
    db: Database,
    lease: Lease,
    provider: SqlProvider,
    objects: PayloadObjects,
  ): Promise<SqlStorage> {
    const head = (
      await db.query("SELECT epoch,owner,next_id,next_seq FROM harness WHERE ns=$1", [lease.ns])
    )[0];

    check(head, "Missing ownership head");

    check(integer(head.epoch) === lease.epoch && head.owner === lease.owner, "Stale owner at open");

    const nextId = headCounter(head.next_id);
    const nextSeq = headCounter(head.next_seq);

    check(nextId >= 2 && nextSeq >= 1, "Invalid head counters");

    return new SqlStorage(db, lease, provider, objects, nextSeq);
  }

  private run<T>(fn: () => Promise<T>, read = true): Promise<T> {
    if (this.state !== "open") return Promise.reject(new Error(`SqlStorage is ${this.state}`));

    return this.gate.run(async () => {
      if (this.state !== "open") throw new Error(`SqlStorage is ${this.state}`);
      if (!read) return fn();

      try {
        await this.q(BEGIN_READ);
        const result = await fn();

        await this.q("COMMIT");

        return result;
      } catch (error) {
        await this.rollback(error);

        throw error;
      }
    });
  }

  private async rollback(original?: unknown): Promise<void> {
    try {
      await this.q("ROLLBACK");
    } catch (error) {
      this.state = "failed";

      throw new AggregateError([original, error], "rollback failed", {
        cause: original,
      });
    }
  }

  private q(sql: string, params: readonly SqlValue[] = []): Promise<Row[]> {
    return this.db.query(sql, params);
  }

  /** Pi owns payload semantics; decoded envelopes and SQL indexes must agree. */
  private async decode<K extends RecordType>(row: Row, type: K): Promise<Records[K]> {
    const key = `${this.lease.ns}/records/${String(row.id)}`;
    try {
      check(row.type === type && row.ns === this.lease.ns, "Record namespace/type mismatch");
      const value: unknown = JSON.parse(await this.payloads.decode(row, key));
      validateRecord(value, row, type);
      return value as Records[K];
    } catch (error) {
      if (error instanceof PayloadCorrupt) throw error;
      throw new PayloadCorrupt(key, error);
    }
  }

  private async get<K extends RecordType>(type: K, id: number): Promise<Records[K] | undefined> {
    assertSafeInteger(id);
    const row = (
      await this.q(
        `SELECT ${projection("records")} FROM records WHERE ns=$1 AND id=$2 AND type=$3`,
        [this.lease.ns, id, type],
      )
    )[0];

    return row ? this.decode(row, type) : undefined;
  }

  private async scan<K extends RecordType>(
    type: K,
    clauses: string[],
    params: SqlValue[],
    limit: number,
    cursor: Readonly<Record<string, unknown>> | undefined,
    requested?: ScanOrder,
  ): Promise<Page<Records[K], Cursor>> {
    const { order, after } = scanStart(requested, cursor, "ascending");
    const values: SqlValue[] = [this.lease.ns, type, ...params];

    // Callers use $3 onward for their exact filters.
    values.push(after ?? (order === "ascending" ? -1 : Number.MAX_SAFE_INTEGER + 1));
    clauses.push(`id ${order === "ascending" ? ">" : "<"} $${values.length}`);
    values.push(limit + 1);

    const rows = await this.q(
      `SELECT ${projection("records")} FROM records WHERE ns=$1 AND type=$2 AND ${clauses.join(" AND ")} ORDER BY id ${order === "ascending" ? "ASC" : "DESC"} LIMIT $${values.length}`,
      values,
    );
    const records: Records[K][] = [];

    for (const row of rows) records.push(await this.decode(row, type));

    return page(records, limit, order);
  }

  mintId<I extends Id<string>>(): Promise<I> {
    return this.run(async () => {
      for (let attempt = 1; ; attempt++) {
        try {
          // Autocommit: never expose a candidate before the durable head acknowledges it.
          // bigint arithmetic preserves MAX_SAFE + 1 exactly; mint does not advance record seq.
          const rows = await this.q(
            `UPDATE harness SET next_id=next_id+1
             WHERE ns=$1 AND epoch=$2 AND owner=$3 AND next_seq=$4 AND next_id <= $5::bigint
             RETURNING next_id-1 AS id`,
            [
              this.lease.ns,
              this.lease.epoch,
              this.lease.owner,
              this.nextSeq,
              String(Number.MAX_SAFE_INTEGER),
            ],
          );

          if (rows[0]) return idFromNumber<I>(integer(rows[0].id));
        } catch (error) {
          if (!this.provider.isRetryableAbort(error)) {
            // A lost autocommit response may have consumed an ID. No retry or compensation.
            this.state = "failed";

            throw error;
          }

          if (attempt === 3) throw error;
          await sleep(20 * attempt);
          continue;
        }

        const head = (
          await this.q("SELECT epoch,owner,next_seq,next_id FROM harness WHERE ns=$1", [
            this.lease.ns,
          ])
        )[0];

        if (
          !head ||
          integer(head.epoch) !== this.lease.epoch ||
          head.owner !== this.lease.owner ||
          headCounter(head.next_seq) !== this.nextSeq
        ) {
          this.state = "failed";

          throw new Error("Stale owner epoch or sequence");
        }

        check(String(head.next_id) === "9007199254740992", "Invalid allocation head");

        throw new Error("ID space is exhausted");
      }
    }, false);
  }

  conversation(id: ConversationId, _context: Context): Promise<ConversationRecord | undefined> {
    return this.run(() => this.get("conversation", id));
  }

  async scanConversations(
    query: ConversationQuery,
    limit: number,
    cursor: Cursor | undefined,
    _context: Context,
  ): Promise<Page<ConversationRecord, Cursor>> {
    const { ownerConversationId, ownerTaskId, order } = query;
    const position =
      cursor === undefined ? undefined : { after: cursor.after, order: cursor.order };
    return this.run(() => {
      const clauses: string[] = [];
      const params: SqlValue[] = [];
      if (ownerConversationId !== undefined) {
        assertSafeInteger(ownerConversationId);
        params.push(ownerConversationId);
        clauses.push(`owner_conversation_id=$${params.length + 2}`);
      }
      if (ownerTaskId !== undefined) {
        assertSafeInteger(ownerTaskId);
        params.push(ownerTaskId);
        clauses.push(`owner_task_id=$${params.length + 2}`);
      }
      return this.scan("conversation", clauses, params, limit, position, order);
    });
  }

  private async chain(
    id: ConversationId,
  ): Promise<{ record: ConversationRecord; upper: number }[]> {
    const chain: { record: ConversationRecord; upper: number }[] = [];
    let upper = Number.MAX_SAFE_INTEGER;
    const seen = new Set<number>();

    while (true) {
      check(!seen.has(id), "Conversation ancestry cycle");
      seen.add(id);

      const record = await this.get("conversation", id);

      if (!record) throw new StorageRequestError(`Unknown conversation: ${id}`);
      chain.push({ record, upper });
      if (!record.parent) return chain;
      upper = Math.min(upper, record.parent.at);
      id = record.parent.conversationId;
    }
  }

  entry(id: EntryId, context: Context): Promise<{ entry: EntryRecord; commitSeq: Seq } | undefined>;

  entry(
    conversationId: ConversationId,
    id: EntryId,
    context: Context,
  ): Promise<{ entry: EntryRecord; commitSeq: Seq } | undefined>;

  entry(
    first: EntryId | ConversationId,
    second: EntryId | Context,
    third?: Context,
  ): Promise<{ entry: EntryRecord; commitSeq: Seq } | undefined> {
    return this.run(async () => {
      const id = third === undefined ? first : typeof second === "number" ? second : undefined;

      if (id === undefined) throw new StorageRequestError("Missing entry ID");
      assertSafeInteger(id);

      const chain =
        third === undefined ? undefined : await this.chain(idFromNumber<ConversationId>(first));
      const row = (
        await this.q(
          `SELECT ${projection("records")} FROM records WHERE ns=$1 AND type='entry' AND id=$2`,
          [this.lease.ns, id],
        )
      )[0];

      if (!row) return undefined;

      const entry = await this.decode(row, "entry");

      if (chain && !chain.some((s) => s.record.id === entry.conversationId && entry.id <= s.upper))
        return undefined;

      return { entry, commitSeq: seqFromNumber(integer(row.commit_seq)) };
    });
  }

  async scanEntries(
    query: EntryQuery,
    limit: number,
    cursor: Cursor | undefined,
    _context: Context,
  ): Promise<Page<EntryRecord, Cursor>> {
    const { conversationId, minEntryId, maxEntryId, order: requested } = query;
    const position =
      cursor === undefined ? undefined : { after: cursor.after, order: cursor.order };
    return this.run(async () => {
      if (minEntryId !== undefined) assertSafeInteger(minEntryId);
      if (maxEntryId !== undefined) assertSafeInteger(maxEntryId);
      const { order, after } = scanStart(requested, position, "descending");
      const segments = await this.chain(conversationId);

      if (order === "ascending") segments.reverse();

      const records: EntryRecord[] = [];

      for (const segment of segments) {
        const lower = Math.max(
          minEntryId ?? 0,
          order === "ascending" && after !== undefined ? after + 1 : 0,
        );
        const upper = Math.min(
          segment.upper,
          maxEntryId ?? Number.MAX_SAFE_INTEGER,
          order === "descending" && after !== undefined ? after - 1 : Number.MAX_SAFE_INTEGER,
        );
        const rows = await this.q(
          `SELECT ${projection("records")} FROM records WHERE ns=$1 AND type='entry' AND conversation_id=$2 AND id >= $3 AND id <= $4 ORDER BY id ${order === "ascending" ? "ASC" : "DESC"} LIMIT $5`,
          [this.lease.ns, segment.record.id, lower, upper, limit + 1 - records.length],
        );

        for (const row of rows) records.push(await this.decode(row, "entry"));
        if (records.length > limit) break;
      }

      return page(records, limit, order);
    });
  }

  findLatestHeadMarker(
    id: ConversationId,
    cutoff: EntryId | undefined,
    _context: Context,
  ): Promise<(EntryRecord & { head: EntryId }) | undefined> {
    return this.run(async () => {
      if (cutoff !== undefined) assertSafeInteger(cutoff);
      for (const segment of await this.chain(id)) {
        const row = (
          await this.q(
            `SELECT ${projection("records")} FROM records WHERE ns=$1 AND type='entry' AND conversation_id=$2 AND has_head=true AND id<=$3 ORDER BY id DESC LIMIT 1`,
            [
              this.lease.ns,
              segment.record.id,
              Math.min(segment.upper, cutoff ?? Number.MAX_SAFE_INTEGER),
            ],
          )
        )[0];

        if (row) {
          const entry = await this.decode(row, "entry");

          check(entry.head !== undefined, "Head index mismatch");

          return { ...entry, head: entry.head };
        }
      }

      return undefined;
    });
  }

  private recordMutation(write: HistoryWrite, seq: Seq): Mutation {
    const columns = [
      "ns",
      "id",
      "type",
      "conversation_id",
      "owner_conversation_id",
      "owner_task_id",
      "kind_hash",
      "status",
      "abort_requested",
      "background",
      "request_hash",
      "has_head",
      "commit_seq",
      "address_hash",
      "scope_kind",
      "scope_owner",
      "created_at",
      "retired_at",
      ...PAYLOAD_COLUMNS,
    ];
    const params: SqlValue[] = [
      this.lease.ns,
      write.value.id,
      write.type,
      write.type === "entry" ? write.value.conversationId : null,
      write.type === "conversation" ? (write.value.owner?.conversationId ?? null) : null,
      write.type === "conversation" ? (write.value.owner?.taskId ?? null) : null,
      write.type === "entry" ? computeSha256(JSON.stringify(write.value.kind)) : null,
      null,
      null,
      null,
      null,
      write.type === "entry" ? write.value.head !== undefined : null,
      write.type === "entry" ? seq : null,
      null,
      null,
      null,
      null,
      null,
    ];

    return {
      sql: `INSERT INTO records(${columns.join(",")}) VALUES(${columns.map((_, i) => "$" + (i + 1)).join(",")})`,
      params,
      payload: {
        table: "records",
        ns: this.lease.ns,
        id: write.value.id,
        json: JSON.stringify(write.value),
      },
    };
  }

  private prepare(writes: readonly HistoryWrite[], seq: Seq): Promise<Mutation[]> {
    return this.validateRecordWrites(writes, seq);
  }

  /** One bounded ownership query in the repeatable snapshot, before publication. */
  private async validateRecordWrites(
    writes: readonly HistoryWrite[],
    seq: Seq,
  ): Promise<Mutation[]> {
    const ids = writes.map((write) => write.value.id);
    const claimed = new Set<number>();
    for (const id of ids) {
      check(!claimed.has(id), `ID ${id} is written more than once`);
      claimed.add(id);
    }

    if (ids.length > 0) {
      // freezeWrites allows at most 999 numeric ID binds, plus the namespace.
      const placeholders = ids.map((_, i) => `$${i + 2}`).join(",");
      const existing = (
        await this.q(
          `SELECT id,type FROM records WHERE ns=$1 AND id IN (${placeholders}) ORDER BY id LIMIT 1`,
          [this.lease.ns, ...ids],
        )
      )[0];
      check(!existing, `ID ${String(existing?.id)} already belongs to ${String(existing?.type)}`);
    }
    return writes.map((write) => this.recordMutation(write, seq));
  }

  private async validateSnapshot(frozen: readonly HistoryWrite[], seq: Seq): Promise<Mutation[]> {
    // One snapshot plus the next_seq fence protects all prepared results.
    // Uploads never run inside a DB transaction.
    try {
      await this.q(BEGIN_READ);
      const mutations = await this.prepare(frozen, seq);

      await this.q("COMMIT");

      return mutations;
    } catch (error) {
      await this.rollback(error);

      throw error;
    }
  }

  private planPayload(mutations: Mutation[]): PayloadPlan {
    const metadataBytes = mutations.reduce(
      (n, m) =>
        n + Buffer.byteLength(m.sql) + 3 * Buffer.byteLength(JSON.stringify(m.params)) + 2048,
      2048,
    );
    const plan = this.payloads.plan(
      mutations.map((m) => m.payload),
      metadataBytes,
    );

    assertEnvelope(1 + plan.rowMutations, plan.estimatedBytes);

    return plan;
  }

  private async upload(mutations: Mutation[], plan: PayloadPlan) {
    const payloads = await plan.upload();

    check(payloads.length === mutations.length, "Payload plan length mismatch");

    const statements = mutations
      .flatMap((m, i) => {
        const p = payloads[i]!;

        check(p.params.length === PAYLOAD_COLUMNS.length, "Payload column count mismatch");

        return [...p.before, { sql: m.sql, params: [...m.params, ...p.params] }, ...p.after];
      })
      .map((s) => Object.freeze({ sql: s.sql, params: Object.freeze([...s.params]) }));

    check(statements.length === plan.rowMutations, "Payload statement accounting mismatch");

    return statements;
  }

  /** Only this frozen batch is retried. Unknown COMMIT never becomes a rollback claim. */
  private async publishFrozen(
    statements: readonly { readonly sql: string; readonly params: readonly SqlValue[] }[],
    nextId: number,
    seq: Seq,
  ): Promise<Seq> {
    for (let attempt = 1; ; attempt++) {
      let commitSent = false;

      try {
        await this.q(BEGIN_WRITE);

        const fence = await this.q(
          `UPDATE harness SET next_seq=next_seq+1,next_id=GREATEST(next_id,$5)
                 WHERE ns=$1 AND epoch=$2 AND owner=$3 AND next_seq=$4 RETURNING next_seq`,
          [this.lease.ns, this.lease.epoch, this.lease.owner, this.nextSeq, nextId],
        );

        if (fence.length !== 1) {
          this.state = "failed";
          throw new Error("Stale owner epoch or sequence");
        }
        for (const statement of statements) await this.q(statement.sql, statement.params);
        commitSent = true;
        await this.q("COMMIT");
        this.nextSeq++;

        return seq;
      } catch (error) {
        const aborted = this.provider.isRetryableAbort(error);

        if (commitSent && !aborted) {
          this.state = "failed";

          throw error;
        }

        await this.rollback(error);
        if (!aborted || attempt >= 3) throw error;
        await sleep(20 * attempt);
      }
    }
  }

  /** Detach and validate at invocation, before the serialized operation's first await. */
  async commit(writes: readonly HistoryWrite[], _context: Context): Promise<Seq> {
    const frozen = freeze(writes);
    for (const write of frozen) {
      check(write.type === "conversation" || write.type === "entry", "Invalid history write type");
      assertId(write.value.id);
      if (write.type === "entry") {
        assertId(write.value.conversationId);
        if (write.value.head !== undefined) assertPoint(write.value.head);
      } else {
        if (write.value.owner !== undefined) {
          assertId(write.value.owner.conversationId);
          assertId(write.value.owner.taskId);
        }
        if (write.value.parent !== undefined) {
          assertId(write.value.parent.conversationId);
          assertPoint(write.value.parent.at);
        }
      }
    }

    return this.run(async () => {
      check(Number.isSafeInteger(this.nextSeq), "Commit sequence space is exhausted");
      const seq = seqFromNumber(this.nextSeq);
      const nextId = storedHighWater(frozen);
      const mutations = await this.validateSnapshot(frozen, seq);
      const plan = this.planPayload(mutations);
      const statements = await this.upload(mutations, plan);

      return this.publishFrozen(statements, nextId, seq);
    }, false);
  }

  close(_context: Context): Promise<void> {
    if (!this.closing) {
      this.state = "closed";
      this.closing = this.gate.run(() => this.db.end());
    }

    return this.closing;
  }
}
