import { createMemoryBackend } from "../../src/index.ts";
import { harnessId, requestId, check } from "../../src/domain.ts";
import { input, publishSubmission, type ContractFixture } from "./storage.contract.ts";
import { idFromNumber } from "../../src/ids.ts";
import { LEASE_MARGIN_MS } from "../../src/lease.ts";

export async function memoryFixture(
  idle = false,
): Promise<ContractFixture & { advance(ms: number): void; time(): number }> {
  let time = 100000;
  const backend = createMemoryBackend({ now: () => time });
  const authority = await backend.connectAuthority();
  const ns = harnessId("test");
  const seed = (await authority.receive(ns, requestId("seed"), input)).item;
  const claim = await authority.claim(ns, 45000);
  check(claim.kind === "claimed" && seed.kind === "received", "fixture claim");
  if (idle) {
    await publishSubmission({ backend, lease: claim.lease }, 2);
    await authority.markAdmitted(seed, idFromNumber(2));
  }
  return {
    backend,
    authority,
    ns,
    lease: claim.lease,
    firstId: idle ? 3 : 2,
    orderingTies: async () => {},
    setExpiredLeases: async (expiries) => {
      // Issue real public claims at one controlled time, then expire every claim.
      const at = Math.min(...expiries.map((l) => l.until)) - LEASE_MARGIN_MS - 1;
      check(at >= time, "Expiry setup must advance the fixture clock");
      time = at;
      const actual = [];
      for (const { ns, until } of expiries) {
        const claimed = await authority.claim(ns, until - time - LEASE_MARGIN_MS);
        check(claimed.kind === "claimed", "fixture expiry claim");
        actual.push({ ns: claimed.lease.ns, until: claimed.lease.until });
      }
      time = Math.max(...actual.map((l) => l.until)) + 1;
      return actual;
    },
    advanceAuthorityClock: async (ms) => {
      time += ms;
    },
    expire: async () => {
      time += 900001;
    },
    advance: (ms) => {
      time += ms;
    },
    time: () => time,
    finish: () => authority.close(),
  };
}
