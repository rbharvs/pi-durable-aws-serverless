import type { Context } from "@earendil-works/chord";
import { apply, assertValidOp, type Op } from "@earendil-works/chord/delta";
import { StorageRequestError } from "@earendil-works/pi-durable";
import { idFromNumber, seqFromNumber } from "../ids.ts";
import { scanStart, nextCursor } from "./scan.ts";
import type {
  Storage,
  StorageWrite,
  DocumentId,
  DocumentRecord,
  DocumentAddress,
  DocumentCreate,
  DocumentContent,
  DocumentPoint,
  DocumentQuery,
  StoredDocument,
  Seq,
  Id,
  ConversationId,
  ConversationRecord,
  ConversationQuery,
  EntryId,
  EntryRecord,
  EntryQuery,
  TaskId,
  TaskQuery,
  SubmissionId,
  SubmissionRecord,
  SubmissionQuery,
  Cursor,
  Page,
  ScanOrder,
} from "@earendil-works/pi-durable";
import { BEGIN_WRITE, BEGIN_READ, Database, type Row, type SqlValue } from "./db.ts";
import type { SqlProvider } from "./connection.ts";
import {
  Payloads,
  projection,
  referenceProjection,
  PAYLOAD_COLUMNS,
  PayloadCorrupt,
  type PayloadInput,
  type PayloadPlan,
} from "./payloads.ts";
import { assertEnvelope, EnvelopeRejected } from "../payload.ts";
import { LIMITS } from "../limits.ts";
import type { PayloadObjects } from "../s3-payload.ts";
import type { Lease } from "../lease.ts";
import { freezeWrites as freeze, storedHighWater, writeId } from "../write-bounds.ts";
import {
  writeShape,
  validateRecord,
  jsonObject,
  assertJson,
  scope,
  addressHash,
  currentOnly,
  alive,
  type RecordType,
  type Records,
} from "./records.ts";
import { check, integer, computeSha256, Mutex, sleep } from "../domain.ts";

type StoredTask = Records["task"];

type Mutation = { sql: string; params: SqlValue[]; payload: PayloadInput };

type DocumentAction = {
  create?: DocumentCreate;
  copy?: { id: DocumentId; at: DocumentPoint };
  content?: DocumentContent;
  retire: boolean;
};

type AdapterState = "open" | "failed" | "closed";

/** Selectors are safe numbers, including empty/negative bounds; never coerce decimal strings. */
function assertSafeInteger(value: unknown): asserts value is number {
  if (typeof value !== "number" || !Number.isSafeInteger(value))
    throw new StorageRequestError("Expected safe integer number");
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
export class SqlStorage implements Storage {
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

  task(id: TaskId, _context: Context): Promise<StoredTask | undefined> {
    return this.run(() => this.get("task", id));
  }

  submission(id: SubmissionId, _context: Context): Promise<SubmissionRecord | undefined> {
    return this.run(() => this.get("submission", id));
  }

  async scanTasks(
    query: TaskQuery,
    limit: number,
    cursor: Cursor | undefined,
    _context: Context,
  ): Promise<Page<StoredTask, Cursor>> {
    const { conversationId, kind, status, abortRequested, background, order } = query;
    const position =
      cursor === undefined ? undefined : { after: cursor.after, order: cursor.order };
    return this.run(() => {
      if (conversationId !== undefined) assertSafeInteger(conversationId);
      const clauses: string[] = [];
      const params: SqlValue[] = [];
      for (const [column, value] of [
        ["conversation_id", conversationId],
        ["kind_hash", kind === undefined ? undefined : computeSha256(JSON.stringify(kind))],
        ["status", status],
        ["abort_requested", abortRequested],
        ["background", background],
      ] as const) {
        if (value !== undefined) {
          params.push(value);
          clauses.push(`${column}=$${params.length + 2}`);
        }
      }
      return this.scan("task", clauses, params, limit, position, order);
    });
  }

  async scanSubmissions(
    query: SubmissionQuery,
    limit: number,
    cursor: Cursor | undefined,
    _context: Context,
  ): Promise<Page<SubmissionRecord, Cursor>> {
    const { conversationId, status, order } = query;
    const position =
      cursor === undefined ? undefined : { after: cursor.after, order: cursor.order };
    return this.run(() => {
      const clauses: string[] = [];
      const params: SqlValue[] = [];
      if (conversationId !== undefined) {
        assertSafeInteger(conversationId);
        params.push(conversationId);
        clauses.push(`conversation_id=$${params.length + 2}`);
      }
      if (status !== undefined) {
        params.push(status);
        clauses.push(`status=$${params.length + 2}`);
      }
      return this.scan("submission", clauses, params, limit, position, order);
    });
  }

  submissionByRequest(
    conversationId: ConversationId,
    requestId: string,
    _context: Context,
  ): Promise<SubmissionRecord | undefined> {
    return this.run(async () => {
      assertSafeInteger(conversationId);
      const rows = await this.q(
        `SELECT ${projection("records")} FROM records WHERE ns=$1 AND type='submission' AND conversation_id=$2 AND request_hash=$3`,
        [this.lease.ns, conversationId, computeSha256(JSON.stringify(requestId))],
      );
      for (const row of rows) {
        const value = await this.decode(row, "submission");
        if (value.requestId === requestId) return value;
      }
      return undefined;
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

  private async find(
    address: DocumentAddress,
    at: DocumentPoint,
  ): Promise<DocumentRecord | undefined> {
    if (at !== "current") assertSafeInteger(at);
    assertSafeInteger(scope(address.scope)[1]);
    const rows = await this.q(
      `SELECT ${projection("records")} FROM records WHERE ns=$1 AND type='document' AND address_hash=$2 AND created_at<=$3 AND (retired_at IS NULL OR retired_at>$4) ORDER BY created_at DESC`,
      [
        this.lease.ns,
        addressHash(address),
        at === "current" ? Number.MAX_SAFE_INTEGER : at,
        at === "current" ? Number.MAX_SAFE_INTEGER : at,
      ],
    );

    for (const row of rows) {
      const record = await this.decode(row, "document");

      if (
        record.kind === address.kind &&
        record.key === address.key &&
        JSON.stringify(scope(record.scope)) === JSON.stringify(scope(address.scope))
      )
        return record;
    }

    return undefined;
  }

  findDocument(
    address: DocumentAddress,
    at: DocumentPoint,
    _context: Context,
  ): Promise<DocumentRecord | undefined> {
    const detached = structuredClone(address);
    return this.run(() => this.find(detached, at));
  }

  scanDocuments(
    query: DocumentQuery,
    limit: number,
    cursor: Cursor | undefined,
    _context: Context,
  ): Promise<Page<DocumentRecord, Cursor>> {
    const detached = structuredClone(query);
    const position =
      cursor === undefined ? undefined : { after: cursor.after, order: cursor.order };
    return this.run(async () => {
      const [kind, owner] = scope(detached.scope);
      assertSafeInteger(owner);
      const at = detached.at === "current" ? Number.MAX_SAFE_INTEGER : detached.at;
      assertSafeInteger(at);
      const clauses = [
        "scope_kind=$3",
        "scope_owner=$4",
        "created_at<=$5",
        "(retired_at IS NULL OR retired_at>$5)",
      ];
      const params: SqlValue[] = [kind, owner, at];

      if (detached.kind !== undefined) {
        params.push(computeSha256(JSON.stringify(detached.kind)));
        clauses.push("kind_hash=$6");
      }

      return this.scan("document", clauses, params, limit, position);
    });
  }

  private async materialize(
    id: DocumentId,
    at: DocumentPoint,
  ): Promise<StoredDocument | undefined> {
    if (at !== "current") assertSafeInteger(at);
    const record = await this.get("document", id);

    if (!record) return undefined;
    if (at !== "current" && currentOnly(record))
      throw new StorageRequestError(`Document ${id} does not retain historical content`);
    if (!alive(record, at)) return undefined;

    try {
      const upper = at === "current" ? Number.MAX_SAFE_INTEGER : at;
      const base = (
        await this.q(
          `SELECT ${projection("revisions")} FROM revisions WHERE ns=$1 AND document_id=$2 AND kind='base' AND seq<=$3 ORDER BY seq DESC LIMIT 1`,
          [this.lease.ns, id, upper],
        )
      )[0];

      check(base, `Document ${id} is missing a required base`);

      const version = base.version;
      check(typeof version === "number" && Number.isFinite(version), "Invalid document version");
      let value = jsonObject(
        JSON.parse(
          await this.payloads.decode(base, `${this.lease.ns}/revisions/${id}/${String(base.seq)}`),
        ),
      );
      const tail = await this.q(
        `SELECT ${projection("revisions")} FROM revisions WHERE ns=$1 AND document_id=$2 AND seq>$3 AND seq<=$4 ORDER BY seq`,
        [this.lease.ns, id, integer(base.seq), upper],
      );

      for (const revision of tail) {
        check(
          revision.kind === "delta" && revision.version === version,
          `Document ${id} crosses a stored version boundary without a base`,
        );

        const ops: unknown = JSON.parse(
          await this.payloads.decode(
            revision,
            `${this.lease.ns}/revisions/${id}/${String(revision.seq)}`,
          ),
        );

        check(Array.isArray(ops), "Invalid delta");
        assertJson(ops);

        const validated: Op[] = [];

        for (const op of ops) {
          assertValidOp(op);
          validated.push(op);
        }

        // Parsed/validated delta data preserves JSON; only root replacement can change
        // the document container. Check each revision without rewalking its unchanged tree.
        value = apply(value, validated);
        check(
          typeof value === "object" && value !== null && !Array.isArray(value),
          "Invalid document value",
        );
      }

      return {
        record,
        version,
        value,
        deltasSinceBase: tail.length,
      };
    } catch (error) {
      if (error instanceof PayloadCorrupt) throw error;
      throw new PayloadCorrupt(`${this.lease.ns}/revisions/${id}`, error);
    }
  }

  document(
    id: DocumentId,
    at: DocumentPoint,
    _context: Context,
  ): Promise<StoredDocument | undefined> {
    return this.run(() => this.materialize(id, at));
  }

  private recordMutation(
    write:
      | Extract<StorageWrite, { type: "conversation" | "entry" | "task" | "submission" }>
      | { type: "document"; value: DocumentRecord },
    seq: Seq,
    previous?: Row,
  ): Mutation {
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
      write.type !== "conversation" && write.type !== "document"
        ? write.value.conversationId
        : null,
      write.type === "conversation" ? (write.value.owner?.conversationId ?? null) : null,
      write.type === "conversation" ? (write.value.owner?.taskId ?? null) : null,
      write.type === "entry" || write.type === "task" || write.type === "document"
        ? computeSha256(JSON.stringify(write.value.kind))
        : null,
      write.type === "task"
        ? write.value.state.status
        : write.type === "submission"
          ? write.value.status
          : null,
      write.type === "task" ? write.value.abortRequested : null,
      write.type === "task" ? write.value.background : null,
      write.type === "submission" && write.value.requestId !== undefined
        ? computeSha256(JSON.stringify(write.value.requestId))
        : null,
      write.type === "entry" ? write.value.head !== undefined : null,
      write.type === "entry" ? seq : null,
      write.type === "document" ? addressHash(write.value) : null,
      write.type === "document" ? scope(write.value.scope)[0] : null,
      write.type === "document" ? scope(write.value.scope)[1] : null,
      write.type === "document" ? write.value.createdAt : null,
      write.type === "document" ? (write.value.retiredAt ?? null) : null,
    ];

    return {
      sql: `INSERT INTO records(${columns.join(",")}) VALUES(${columns.map((_, i) => "$" + (i + 1)).join(",")})${
        write.type === "task" || write.type === "submission" || write.type === "document"
          ? " ON CONFLICT(ns,id) DO UPDATE SET " +
            columns
              .slice(3)
              .map((c) => `${c}=excluded.${c}`)
              .join(",")
          : ""
      }`,
      params,
      payload: {
        table: "records",
        ns: this.lease.ns,
        id: write.value.id,
        json: JSON.stringify(write.value),
        ...(previous ? { previous } : {}),
      },
    };
  }

  private actions(writes: readonly StorageWrite[]): Map<DocumentId, DocumentAction> {
    const actions = new Map<DocumentId, DocumentAction>();

    for (const write of writes) {
      switch (write.type) {
        case "conversation":
        case "entry":
        case "task":
        case "submission":
          continue;

        case "document.create":
        case "document.copy":
        case "document.change":
        case "document.retire":
          break;

        default: {
          const unexpected: never = write;

          throw new Error(`Unsupported write: ${String(unexpected)}`);
        }
      }

      const id = idFromNumber<DocumentId>(writeId(write));
      let action = actions.get(id);

      if (!action) {
        action = { retire: false };
        actions.set(id, action);
      }

      if (write.type === "document.retire") {
        check(!action.retire, `Document ${id} is retired more than once`);
        action.retire = true;
      } else {
        check(
          !action.content && !action.copy && !action.create,
          `Document ${id} has more than one content command`,
        );
        if (write.type === "document.create" || write.type === "document.copy")
          action.create = write.record;
        if (write.type === "document.copy") action.copy = write.source;
        else action.content = write.content;
      }
    }

    return actions;
  }

  private async prepare(writes: readonly StorageWrite[], seq: Seq): Promise<Mutation[]> {
    const mutations: Mutation[] = [];
    let logicalBytes = 0;
    const addMutation = (mutation: Mutation): void => {
      // Count each serialized owner payload once, including resolved copies and repeated writes.
      // Reject incrementally before retaining the rest of the batch or publishing any objects.
      logicalBytes += Buffer.byteLength(mutation.payload.json);
      if (logicalBytes > LIMITS.maxLogicalCommitBytes)
        throw new EnvelopeRejected("Resolved logical commit exceeds 32 MiB bound");
      mutations.push(mutation);
    };
    const existing = await this.validateRecordWrites(writes, seq, addMutation);
    const actions = this.actions(writes);
    const documents = await this.validateDocumentActions(actions, seq);
    await this.planDocumentActions(actions, documents, existing, seq, addMutation);
    return mutations;
  }

  /** One bounded ownership query in the same snapshot as document validation and copy reads. */
  private async validateRecordWrites(
    writes: readonly StorageWrite[],
    seq: Seq,
    addMutation: (mutation: Mutation) => void,
  ): Promise<Map<number, Row>> {
    const ids = [...new Set(writes.map(writeId))];
    const existing = new Map<number, Row>();
    if (ids.length > 0) {
      const placeholders = ids.map((_, i) => `$${i + 2}`).join(",");
      const rows = await this.q(
        `SELECT ${referenceProjection("records")} FROM records WHERE ns=$1 AND id IN (${placeholders}) ORDER BY id`,
        [this.lease.ns, ...ids],
      );
      for (const row of rows) existing.set(integer(row.id), row);
    }
    const claimed = new Map<number, RecordType>();
    for (const write of writes) {
      if (write.type === "document.change" || write.type === "document.retire") {
        const prior = existing.get(write.id);
        check(!prior || prior.type === "document", `ID ${write.id} belongs to another record type`);
        continue;
      }
      const document = write.type === "document.create" || write.type === "document.copy";
      const record = document ? write.record : write.value;
      const type = document ? "document" : write.type;
      const prior = existing.get(record.id);
      const earlier = claimed.get(record.id);
      if (type === "conversation" || type === "entry" || type === "document") {
        check(!earlier, `ID ${record.id} is written more than once`);
        check(!prior, `ID ${record.id} already belongs to ${String(prior?.type)}`);
      } else {
        check(!earlier || earlier === type, `ID ${record.id} is written as two record types`);
        check(
          !prior || prior.type === type,
          `ID ${record.id} already belongs to ${String(prior?.type)}`,
        );
      }
      claimed.set(record.id, type);
      if (!document) addMutation(this.recordMutation(write, seq, prior));
    }
    return existing;
  }

  /** Address liveness and lifetime validation share one snapshot with copy planning. */
  private async validateDocumentActions(
    actions: Map<DocumentId, DocumentAction>,
    seq: Seq,
  ): Promise<Map<DocumentId, DocumentRecord>> {
    const liveCounts = new Map<string, number>();
    const documents = new Map<DocumentId, DocumentRecord>();

    for (const [id, action] of actions) {
      check(
        !action.copy || !actions.has(action.copy.id),
        `Document copy ${id} source is changed in the copy batch`,
      );

      // The ownership query already rejected creation over any stored ID.
      const existing = action.create ? undefined : await this.get("document", id);

      check(action.create || existing, `Unknown document: ${id}`);
      check(existing?.retiredAt === undefined, `Document ${id} is retired`);

      if (action.content?.kind === "delta") {
        const previous = (
          await this.q(
            "SELECT version FROM revisions WHERE ns=$1 AND document_id=$2 ORDER BY seq DESC LIMIT 1",
            [this.lease.ns, id],
          )
        )[0];

        check(previous, `Document ${id} delta has no base`);
        check(
          previous.version === action.content.version,
          `Document ${id} version transition requires a base`,
        );
      }

      const record: DocumentRecord = action.create
        ? {
            ...action.create,
            createdAt: seq,
            ...(action.retire ? { retiredAt: seq } : {}),
          }
        : existing!;

      documents.set(id, record);

      const key = addressHash(record);
      let live = liveCounts.get(key);

      // A stored incarnation that is not retired is the current one at its address.
      if (live === undefined) live = existing || (await this.find(record, "current")) ? 1 : 0;
      if (action.retire && existing) live--;
      if (action.create && !action.retire) live++;
      liveCounts.set(key, live);
    }

    for (const count of liveCounts.values())
      check(count <= 1, "Document address already has a current incarnation");

    return documents;
  }

  private async planDocumentActions(
    actions: Map<DocumentId, DocumentAction>,
    documents: Map<DocumentId, DocumentRecord>,
    existing: Map<number, Row>,
    seq: Seq,
    addMutation: (mutation: Mutation) => void,
  ): Promise<void> {
    for (const [id, action] of actions) {
      let content = action.content;
      const record = documents.get(id)!;

      if (action.copy) {
        try {
          const source = await this.materialize(action.copy.id, action.copy.at);

          check(
            source &&
              source.record.scope.kind === "conversation" &&
              record.scope.kind === "conversation" &&
              source.record.kind === record.kind &&
              source.record.key === record.key &&
              source.record.history === record.history &&
              source.record.fork === record.fork,
            "Copy source mismatch",
          );
          content = {
            kind: "base",
            version: source.version,
            value: source.value,
          };
        } catch (error) {
          throw new Error(`Document copy ${id} was rejected`, { cause: error });
        }
      }

      if (action.create || action.retire)
        addMutation(
          this.recordMutation(
            {
              type: "document",
              value: action.retire ? { ...record, retiredAt: seq } : record,
            },
            seq,
            existing.get(id),
          ),
        );
      if (content)
        addMutation({
          sql: `INSERT INTO revisions(ns,document_id,seq,kind,version,${PAYLOAD_COLUMNS.join(",")}) VALUES($1,$2,$3,$4,$5,${PAYLOAD_COLUMNS.map((_, i) => `$${i + 6}`).join(",")})`,
          params: [this.lease.ns, id, seq, content.kind, content.version],
          payload: {
            table: "revisions",
            ns: this.lease.ns,
            id,
            json: JSON.stringify(content.kind === "base" ? content.value : content.ops),
          },
        });
      // Retain old revisions rather than an unbounded DELETE. Reads select the most recent base.
    }
  }

  private async validateSnapshot(frozen: readonly StorageWrite[], seq: Seq): Promise<Mutation[]> {
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
  async commit(writes: readonly StorageWrite[], _context: Context): Promise<Seq> {
    const frozen = freeze(writes);
    for (const write of frozen) {
      try {
        // freeze() normalized the exact JSON used by index/ID accounting and publication.
        writeShape(write);
      } catch (error) {
        throw new StorageRequestError(`Invalid ${String(write?.type)} write: ${String(error)}`);
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
