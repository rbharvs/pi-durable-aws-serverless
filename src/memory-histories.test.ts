import { it, expect } from "vitest";
import type { Storage } from "@earendil-works/pi-durable";
import { memoryFixture } from "../tests/shared/memory.ts";
import { runHistory, type Operation } from "../tests/shared/histories.ts";

// This prefix, not a uniquely generated tail, protects the mint-local regression.
export const prefix: Operation[] = [
  { kind: "mint" },
  { kind: "reopen" },
  { kind: "takeover" },
  { kind: "unknown-mint", applied: false },
  { kind: "unknown-mint", applied: true },
  { kind: "unknown-commit", applied: false },
  { kind: "unknown-commit", applied: true },
  { kind: "invalid" },
  { kind: "transport-equivalence" },
  { kind: "submit-mark-gap" },
];
const choices: Operation[] = [
  { kind: "mint" },
  { kind: "reopen" },
  { kind: "store" },
  { kind: "invalid" },
  { kind: "takeover" },
  { kind: "transport-equivalence" },
  { kind: "unknown-mint", applied: false },
  { kind: "unknown-mint", applied: true },
  { kind: "unknown-commit", applied: false },
  { kind: "unknown-commit", applied: true },
];

// Small deterministic generator, not a shrinker. Reproduce an exact tail with seed + index.
function histories(seed: number): Operation[][] {
  let state = seed >>> 0;
  const next = () => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return state >>> 0;
  };
  return Array.from({ length: 50 }, () =>
    Array.from({ length: 1 + (next() % 20) }, () => choices[next() % choices.length]!),
  );
}

for (const corruption of ["reused-id", "stored-entry"] as const)
  it(`independent ledger detects actual public adapter corruption: ${corruption}`, async () => {
    const f = await memoryFixture();
    const backend = {
      ...f.backend,
      openStorage: async (lease: Parameters<typeof f.backend.openStorage>[0]) => {
        const storage = await f.backend.openStorage(lease);
        return new Proxy(storage, {
          get(target, key) {
            if (key === "then") return undefined;
            if (key === "mintId" && corruption === "reused-id")
              return async () => {
                await target.mintId();
                return 2;
              };
            if (key === "commit" && corruption === "stored-entry")
              return (
                writes: Parameters<Storage["commit"]>[0],
                context: Parameters<Storage["commit"]>[1],
              ) =>
                target.commit(
                  writes.map((w) =>
                    w.type === "entry" && w.value.kind === "history"
                      ? { ...w, value: { ...w.value, data: "corrupted" } }
                      : w,
                  ),
                  context,
                );
            const value = Reflect.get(target, key);
            return typeof value === "function" ? value.bind(target) : value;
          },
        });
      },
    };
    try {
      const operations: Operation[] =
        corruption === "reused-id" ? [{ kind: "mint" }, { kind: "reopen" }] : [{ kind: "store" }];
      await expect(runHistory({ ...f, backend }, operations)).rejects.toThrow(
        corruption === "reused-id" ? "reused or out of order" : "corrupted",
      );
    } finally {
      await f.finish();
    }
  });

const seeds = process.env.HISTORY_SEED ? [Number(process.env.HISTORY_SEED)] : [20261017, 19790527];
const replay =
  process.env.HISTORY_INDEX === undefined ? undefined : Number(process.env.HISTORY_INDEX);
if (seeds.some((s) => !Number.isSafeInteger(s) || s <= 0 || s > 0xffffffff))
  throw Error("Invalid HISTORY_SEED");
if (replay !== undefined && (!Number.isInteger(replay) || replay < 0 || replay >= 50))
  throw Error("Invalid HISTORY_INDEX");

it("memory mandatory history prefix protects allocation independently of generated tails", async () => {
  const f = await memoryFixture();
  try {
    await expect(runHistory(f, prefix)).resolves.toBeUndefined();
  } finally {
    await f.finish();
  }
});

for (const seed of seeds)
  it(`memory generated histories + independent ledger seed=${seed}`, async () => {
    let count = 0;
    for (const [index, tail] of histories(seed).entries()) {
      if (replay !== undefined && index !== replay) continue;
      const operations = [...prefix, ...tail],
        f = await memoryFixture();
      try {
        await runHistory(f, operations);
        count++;
      } catch (cause) {
        throw new Error(JSON.stringify({ seed, index, operations }), { cause });
      } finally {
        await f.finish();
      }
    }
    expect(count).toBe(replay === undefined ? 50 : 1);
  });
