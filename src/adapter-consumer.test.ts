import { expect, it } from "vitest";
import {
  apiHandler,
  canonical,
  harnessId,
  type ApiDependencies,
  type Authority,
  type CommandObjects,
  type HarnessId,
} from "./index.ts";
import { objectFixture } from "../tests/shared/objects.ts";

it("accepts staged input through an instance-backed public dependency method", async () => {
  const fixture = objectFixture();
  const ns = harnessId("class_factory");
  const input = canonical({ content: "hello", whenBusy: "followUp" });
  const authority: Authority = {
    get: async () => undefined,
    receive: async (ns, request, accepted) => ({
      duplicate: false,
      item: { kind: "received", ns, request, ...accepted, receivedAt: 123 },
    }),
    close: async () => {},
  };

  class Dependencies implements ApiDependencies {
    constructor(private readonly store: ReturnType<typeof objectFixture>) {}

    objects(ns: HarnessId): CommandObjects {
      return this.store.objects(ns);
    }

    async connectAuthority() {
      return authority;
    }

    readonly dispatch = { wake: async () => {} };
    readonly event = () => {};
  }

  const run = apiHandler(new Dependencies(fixture));

  expect(
    await run({ ns, request: "one", command: { kind: "s3", ...fixture.stage(ns, input.json) } }),
  ).toEqual({ ok: true, duplicate: false, receivedAt: 123 });
  expect(fixture.openHandles()).toBe(0);
});
