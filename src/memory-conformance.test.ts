import { registerConformance } from "../tests/shared/conformance.ts";
import { memoryFixture } from "../tests/shared/memory.ts";
registerConformance("memory", async () => {
  const f = await memoryFixture();
  return { openStorage: () => f.backend.openStorage(f.lease), finish: f.finish };
});
