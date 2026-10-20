import { createMemoryBackend } from "../../src/index.ts";
import { harnessId, requestId, check } from "../../src/domain.ts";
import { input, publishSubmission, type ContractFixture } from "./storage.contract.ts";
import { idFromNumber } from "../../src/ids.ts";

export async function memoryFixture(
  idle = false,
): Promise<ContractFixture & { advance(ms: number): void; time(): number }> {
  let time = 100000;
  const backend = createMemoryBackend({ now: () => time });
  const authority = await backend.connectAuthority(),
    ns = harnessId("test");
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
