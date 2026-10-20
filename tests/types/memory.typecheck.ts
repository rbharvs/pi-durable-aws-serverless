import {
  createMemoryBackend,
  type MemoryBackendOptions,
  type Backend,
  type Lease,
} from "../../src/index.ts";
import type { Storage } from "@earendil-works/pi-durable";
type Assert<T extends true> = T;
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
export type MemoryFactory = Assert<Equal<ReturnType<typeof createMemoryBackend>, Backend>>;
export type MemoryOptions = Assert<Equal<keyof MemoryBackendOptions, "now">>;
export type StorageUnchanged = Assert<Equal<Awaited<ReturnType<Backend["openStorage"]>>, Storage>>;
export type BackendKeys = Assert<Equal<keyof Backend, "connectAuthority" | "openStorage">>;

export function publicMemory(clock: () => number) {
  const backend: Backend = createMemoryBackend({ now: clock });
  // @ts-expect-error Named options, not a positional clock.
  createMemoryBackend(clock);
  // @ts-expect-error No state/engine escape.
  void backend.heads;
  // @ts-expect-error Leases require adapter-private issuance identity.
  const forged: Lease = { ns: "n", owner: "x", epoch: 1, until: 0 };
  return { backend, forged };
}
