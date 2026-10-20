import { expect, vi } from "vitest";
import { LIMITS } from "../../src/limits.ts";
import { BEGIN_WRITE, type SqlValue } from "../../src/sql/db.ts";
import { Payloads, type PayloadPlan } from "../../src/sql/payloads.ts";
import type { Query } from "./postgres.ts";

export const parameterBytes = (params: readonly SqlValue[]) =>
  params.reduce<number>((n, p) => n + 4 + (p === null ? 0 : Buffer.byteLength(String(p))), 0);

export const mutations = (trace: readonly Query[]) =>
  trace.filter((q) => /^(INSERT|UPDATE|DELETE) /.test(q.sql));

export function observePlans() {
  const plans: PayloadPlan[] = [];
  const actual = Payloads.prototype.plan;
  const spy = vi.spyOn(Payloads.prototype, "plan").mockImplementation(function (
    this: Payloads,
    values,
    metadata,
  ) {
    const plan = actual.call(this, values, metadata);

    plans.push(plan);

    return plan;
  });

  return { plans, restore: () => spy.mockRestore() };
}

/** Check actual physical SQL against the plan, including primitive pg bind bytes. */
export function assertPublication(trace: readonly Query[], plan: PayloadPlan) {
  expect(trace.filter((q) => q.sql === BEGIN_WRITE)).toHaveLength(1);
  const writes = mutations(trace);

  expect(writes.length).toBe(1 + plan.rowMutations);
  expect(writes.length).toBeLessThanOrEqual(LIMITS.maxRows);
  expect(writes.every((q) => q.acknowledged)).toBe(true);
  const sent = writes.reduce((n, q) => n + Buffer.byteLength(q.sql) + parameterBytes(q.params), 0);

  expect(sent).toBeLessThanOrEqual(plan.estimatedBytes);
  expect(plan.estimatedBytes).toBeLessThanOrEqual(LIMITS.maxTransactionBytes);
  expect(trace.some((q) => q.sql === "COMMIT" && q.acknowledged)).toBe(true);
}
