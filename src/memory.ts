import { MemoryStorage as PiMemoryStorage } from "@earendil-works/pi-durable/storage/memory";
import {
  StorageRequestError,
  type Storage,
  type StorageWrite,
  type Id,
} from "@earendil-works/pi-durable";
import { seqFromNumber } from "./ids.ts";
import type { Backend, Authority, InboxItem, Received } from "./authority.ts";
import {
  check,
  epoch,
  ownerToken,
  harnessId,
  requestId,
  parseSha256,
  Mutex,
  type HarnessId,
  type RequestId,
} from "./domain.ts";
import { issueLease, LEASE_MARGIN_MS, type Lease, type Claim } from "./lease.ts";
import { publishedPayload, verifyInlinePayload } from "./payload.ts";
import { RequestConflict } from "./faults.ts";
import { freezeWrites } from "./write-bounds.ts";
import { writeShape } from "./storage-validation.ts";

type Head = {
  epoch: number;
  owner: string | null;
  until: number;
  nextSeq: number;
  /** Owns allocation across handles; Pi advances it past every applied record and document. */
  engine: PiMemoryStorage;
  receipts: Map<RequestId, InboxItem>;
  tasks: Map<number, boolean>;
  submissions: Map<number, boolean>;
};

function hasWork(head: Head): boolean {
  return (
    [...head.receipts.values()].some((r) => r.kind === "received") ||
    [...head.tasks.values()].some(Boolean) ||
    [...head.submissions.values()].some(Boolean)
  );
}

function updateWork(head: Head, writes: readonly StorageWrite[]): void {
  for (const write of writes) {
    if (write.type === "task")
      head.tasks.set(write.value.id, write.value.state.status !== "terminal");
    if (write.type === "submission")
      head.submissions.set(
        write.value.id,
        write.value.status === "queued" || write.value.status === "placed",
      );
  }
}

function safeSelector(value: unknown): void {
  if (typeof value !== "number" || !Number.isSafeInteger(value))
    throw new StorageRequestError("Expected safe integer number");
}

/** Validate numeric selectors, not Pi's document or scan semantics. Context stays caller-owned. */
function readArguments(key: keyof Storage, args: unknown[]): unknown[] {
  const data = structuredClone(args.slice(0, -1));
  const [first, second] = data;
  if (
    [
      "conversation",
      "entry",
      "task",
      "submission",
      "document",
      "submissionByRequest",
      "findLatestHeadMarker",
    ].includes(key)
  )
    safeSelector(first);
  if (key === "entry" && data.length === 2) safeSelector(second);
  if (key === "findLatestHeadMarker" && second !== undefined) safeSelector(second);
  if ((key === "document" || key === "findDocument") && second !== "current") safeSelector(second);
  if (key.startsWith("scan") || key === "findDocument") {
    const query = first as Record<string, unknown>;
    for (const field of [
      "conversationId",
      "ownerConversationId",
      "ownerTaskId",
      "minEntryId",
      "maxEntryId",
    ])
      if (query[field] !== undefined) safeSelector(query[field]);
    if (key === "scanDocuments" && query.at !== "current") safeSelector(query.at);
    if (query.scope !== undefined) {
      const scope = query.scope as Record<string, unknown>;
      if (scope.kind === "conversation") safeSelector(scope.conversationId);
      if (scope.kind === "task") safeSelector(scope.taskId);
    }
  }
  return [...data, args.at(-1)];
}

export interface MemoryBackendOptions {
  /** Unix epoch milliseconds. Inject a deterministic clock for tests; defaults to Date.now. */
  readonly now?: () => number;
}

/**
 * Process-local backing state, independent of Authority/Storage handles. Not persistent storage.
 * Pi 1.3.0's public prepareCommit/apply, mintId and complete reads execute synchronously. Fence,
 * prepare, apply and head publication MUST stay in one synchronous turn, with no await.
 */
export function createMemoryBackend(options: MemoryBackendOptions = {}): Backend {
  const now = options.now?.bind(options) ?? Date.now;
  const heads = new Map<HarnessId, Head>(),
    issued = new WeakSet<Lease>();
  function time(): number {
    const value = now();
    check(Number.isSafeInteger(value) && value >= 0, "Invalid authority clock");
    return value;
  }
  function head(ns: HarnessId): Head {
    const h = heads.get(ns);
    check(h, "Unknown namespace");
    return h;
  }
  function ensure(ns: HarnessId, at: number): Head {
    let h = heads.get(ns);
    if (!h) {
      h = {
        epoch: 0,
        owner: null,
        until: at - 1,
        nextSeq: 1,
        engine: new PiMemoryStorage(),
        receipts: new Map(),
        tasks: new Map(),
        submissions: new Map(),
      };
      heads.set(ns, h);
    }
    return h;
  }
  function fence(h: Head, lease: Lease, seq?: number): void {
    check(
      issued.has(lease) &&
        h.epoch === lease.epoch &&
        h.owner === lease.owner &&
        (seq === undefined || h.nextSeq === seq),
      "Stale owner epoch or sequence",
    );
  }

  return {
    async connectAuthority(): Promise<Authority> {
      let closed = false;
      const run = <T>(fn: () => T): Promise<T> => {
        try {
          check(!closed, "Authority is closed");
          return Promise.resolve(fn());
        } catch (error) {
          return Promise.reject(error);
        }
      };
      return {
        get: (ns, request) => run(() => structuredClone(heads.get(ns)?.receipts.get(request))),
        receive: (ns, request, input) =>
          run(() => {
            harnessId(ns);
            requestId(request);
            const hash = parseSha256(input.hash),
              source = structuredClone(input.payload);
            let payload;
            if (source.kind === "inline") {
              payload = {
                kind: "inline" as const,
                json: verifyInlinePayload(hash, source.json).json,
              };
            } else {
              payload = publishedPayload(ns, source);
              check(payload.sha256 === hash, "Inbox payload integrity");
            }
            const at = time(),
              h = ensure(ns, at),
              prior = h.receipts.get(request);
            if (prior) {
              if (prior.hash !== hash) throw new RequestConflict();
              return { duplicate: true, item: structuredClone(prior) };
            }
            const item: Received = { kind: "received", ns, request, hash, payload, receivedAt: at };
            h.receipts.set(request, item);
            return { duplicate: false, item: structuredClone(item) };
          }),
        claim: (ns, remainingMs) =>
          run((): Claim => {
            harnessId(ns);
            check(
              Number.isSafeInteger(remainingMs) && remainingMs > 0 && remainingMs <= 900000,
              "Invalid remaining invocation time",
            );
            const h = head(ns),
              at = time();
            if (!(h.until < at)) return { kind: "held" };
            const owner = ownerToken(),
              generation = epoch(h.epoch + 1),
              until = at + remainingMs + LEASE_MARGIN_MS;
            check(Number.isSafeInteger(until), "Invalid lease deadline");
            const lease = issueLease(ns, owner, generation, until);
            issued.add(lease);
            h.epoch = generation;
            h.owner = owner;
            h.until = until;
            return { kind: "claimed", lease };
          }),
        received: (ns) =>
          run(() =>
            structuredClone(
              [...(heads.get(ns)?.receipts.values() ?? [])]
                .filter((r): r is Received => r.kind === "received")
                .sort(
                  (a, b) =>
                    a.receivedAt - b.receivedAt ||
                    (a.request < b.request ? -1 : a.request > b.request ? 1 : 0),
                )
                .slice(0, 8),
            ),
          ),
        markAdmitted: (received, id) =>
          run(() => {
            const { ns, request, hash } = received;
            harnessId(ns);
            requestId(request);
            parseSha256(hash);
            check(
              typeof id === "number" && Number.isSafeInteger(id) && id > 0,
              "Invalid submission ID",
            );
            const h = head(ns),
              prior = h.receipts.get(request);
            check(prior && prior.hash === hash, "Inbox identity changed");
            if (prior.kind === "admitted") {
              check(prior.submission === id, "Admission mapping changed");
              return;
            }
            check(h.submissions.has(id), "Submission missing or admission mapping changed");
            h.receipts.set(request, { ...prior, kind: "admitted", submission: id });
          }),
        hasWork: (ns) => run(() => hasWork(head(ns))),
        eligible: () =>
          run(() => {
            const at = time();
            return [...heads]
              .filter(([, h]) => h.until < at && hasWork(h))
              .sort(([a, x], [b, y]) => x.until - y.until || (a < b ? -1 : a > b ? 1 : 0))
              .slice(0, 5)
              .map(([ns]) => ns);
          }),
        release: (lease) =>
          run(() => {
            const h = heads.get(lease.ns);
            if (!h || !issued.has(lease) || h.epoch !== lease.epoch || h.owner !== lease.owner)
              return "lost";
            const at = time();
            h.owner = null;
            h.until = at;
            return "released";
          }),
        close: async () => {
          closed = true;
        },
      };
    },
    async openStorage(lease): Promise<Storage> {
      const h = head(lease.ns);
      fence(h, lease);
      const gate = new Mutex();
      let nextSeq = h.nextSeq,
        state: "open" | "closed" | "failed" = "open",
        closing: Promise<void> | undefined;
      function run<T>(fn: () => T | Promise<T>): Promise<T> {
        if (state !== "open") return Promise.reject(new Error(`MemoryStorage handle is ${state}`));
        return gate.run(async () => {
          check(state === "open", `MemoryStorage handle is ${state}`);
          return fn();
        });
      }
      function checkFence(): void {
        try {
          fence(h, lease, nextSeq);
        } catch (error) {
          state = "failed";
          throw error;
        }
      }
      const commit: Storage["commit"] = (writes) => {
        let frozen: readonly StorageWrite[];
        try {
          frozen = freezeWrites(writes);
          for (const write of frozen) {
            try {
              writeShape(write);
            } catch (error) {
              throw new StorageRequestError(
                `Invalid ${String(write?.type)} write: ${String(error)}`,
              );
            }
          }
        } catch (error) {
          return Promise.reject(error);
        }
        return run(() => {
          checkFence();
          check(Number.isSafeInteger(nextSeq), "Commit sequence space is exhausted");
          const prepared = h.engine.prepareCommit(frozen, seqFromNumber(nextSeq));
          const seq = prepared.apply();
          updateWork(h, frozen);
          h.nextSeq++;
          nextSeq++;
          return seq;
        });
      };
      const mintId: Storage["mintId"] = <I extends Id<string>>() =>
        run(() => {
          checkFence();
          // Pi allocates synchronously in this turn and rejects exhaustion without wrapping.
          return h.engine.mintId<I>();
        });
      const close: Storage["close"] = () => {
        if (!closing) {
          state = "closed";
          closing = gate.run(async () => {});
        }
        return closing;
      };
      // Only public Pi read capabilities escape, never its backing state, allocator or close.
      const reads = new Set<keyof Storage>([
        "conversation",
        "scanConversations",
        "entry",
        "findLatestHeadMarker",
        "scanEntries",
        "task",
        "scanTasks",
        "submission",
        "scanSubmissions",
        "submissionByRequest",
        "findDocument",
        "document",
        "scanDocuments",
      ]);
      return new Proxy<Storage>({} as Storage, {
        get(_target, key) {
          if (key === "then") return undefined;
          if (key === "commit") return commit;
          if (key === "mintId") return mintId;
          if (key === "close") return close;
          check(reads.has(key as keyof Storage), "Unknown Storage operation");
          return (...args: unknown[]) => {
            try {
              const detached = readArguments(key as keyof Storage, args);
              return run(() => Reflect.apply(Reflect.get(h.engine, key), h.engine, detached));
            } catch (error) {
              return Promise.reject(error);
            }
          };
        },
      });
    },
  };
}
