import { expect, it } from "vitest";
import { reconcile, type ReconcilerDependencies } from "./index.ts";
import { harnessId, type HarnessId } from "./domain.ts";

const candidates = ["one", "two", "three", "four", "five"].map(harnessId);

/** Class methods deliberately depend on their receivers at every port. */
class Authority {
  discoveries = 0;
  closes = 0;
  discoveryError?: Error;
  closeError?: Error;
  candidates = candidates;
  async eligible() {
    this.discoveries++;
    if (this.discoveryError) throw this.discoveryError;
    return this.candidates;
  }
  async close() {
    this.closes++;
    if (this.closeError) throw this.closeError;
  }
}
class Dispatcher {
  attempts: HarnessId[] = [];
  failure?: HarnessId;
  constructor(readonly authority: Authority) {}
  async wake(ns: HarnessId) {
    expect(this.authority.closes).toBe(1);
    this.attempts.push(ns);
    if (this.failure === ns) throw Error("private transport outcome unknown");
  }
}
class Dependencies implements ReconcilerDependencies {
  authority = new Authority();
  dispatch = new Dispatcher(this.authority);
  events: Record<string, unknown>[] = [];
  observerThrows = false;
  async connectAuthority() {
    return this.authority;
  }
  event(name: string, fields: Record<string, unknown>) {
    this.events.push({ name, ...fields });
    if (this.observerThrows) throw Error("observer");
  }
}

it("discovers once, closes once before dispatch and preserves every method receiver", async () => {
  const deps = new Dependencies();
  expect(await reconcile(deps)).toBe(5);
  expect(deps.authority.discoveries).toBe(1);
  expect(deps.authority.closes).toBe(1);
  expect(deps.dispatch.attempts).toEqual(candidates);
  expect(deps.events).toEqual([{ name: "reconcile", eligible: candidates, failed: 0 }]);
});

it("continues after a rejected wake without retry or false delivery claims", async () => {
  const deps = new Dependencies();
  deps.dispatch.failure = candidates[1]!;
  deps.observerThrows = true;
  deps.authority.closeError = Error("private close failure");
  expect(await reconcile(deps)).toBe(5);
  expect(deps.authority.closes).toBe(1);
  expect(deps.dispatch.attempts).toEqual(candidates);
  expect(deps.events).toEqual([
    { name: "cleanup-failed", resource: "authority" },
    { name: "dispatch-failed", ns: candidates[1] },
    { name: "reconcile", eligible: candidates, failed: 1 },
  ]);
});

it("preserves the discovery error over close and observer failures without dispatch", async () => {
  const deps = new Dependencies();
  const cause = Error("primary discovery cause");
  deps.authority.discoveryError = cause;
  deps.authority.closeError = Error("secondary close cause");
  deps.observerThrows = true;
  await expect(reconcile(deps)).rejects.toBe(cause);
  expect(deps.authority.discoveries).toBe(1);
  expect(deps.authority.closes).toBe(1);
  expect(deps.dispatch.attempts).toEqual([]);
  expect(deps.events).toEqual([{ name: "cleanup-failed", resource: "authority" }]);
});

it("propagates acquisition failure without inventing a handle to close or a result", async () => {
  const deps = new Dependencies();
  const cause = Error("acquire");
  deps.connectAuthority = async () => {
    throw cause;
  };
  await expect(reconcile(deps)).rejects.toBe(cause);
  expect(deps.authority.closes).toBe(0);
  expect(deps.dispatch.attempts).toEqual([]);
  expect(deps.events).toEqual([]);
});

it("an empty discovery page closes once and reports zero without wakes", async () => {
  const deps = new Dependencies();
  deps.authority.candidates = [];
  expect(await reconcile(deps)).toBe(0);
  expect(deps.authority.closes).toBe(1);
  expect(deps.dispatch.attempts).toEqual([]);
  expect(deps.events).toEqual([{ name: "reconcile", eligible: [], failed: 0 }]);
});

it("awaits each dispatch before continuing and settling", async () => {
  const deps = new Dependencies();
  const entered = Promise.withResolvers<void>();
  const proceed = Promise.withResolvers<void>();
  const wake = deps.dispatch.wake.bind(deps.dispatch);
  deps.dispatch.wake = async (ns) => {
    await wake(ns);
    if (ns === candidates[0]) {
      entered.resolve();
      await proceed.promise;
    }
  };
  let settled = false;
  const running = reconcile(deps).then((result) => {
    settled = true;
    return result;
  });
  try {
    await entered.promise;
    expect(deps.authority.closes).toBe(1);
    expect(deps.dispatch.attempts).toEqual([candidates[0]]);
    expect(settled).toBe(false);
  } finally {
    proceed.resolve();
    await running;
  }
  expect(await running).toBe(5);
  expect(deps.dispatch.attempts).toEqual(candidates);
});
