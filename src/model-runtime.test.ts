import { createMemoryBackend } from "./memory.ts";
import { harnessId } from "./domain.ts";
import { modelRuntimeTests } from "../tests/shared/model-runtime.ts";

modelRuntimeTests(async () => {
  let now = 10000;
  return {
    backend: createMemoryBackend({ now: () => now++ }),
    ns: harnessId("runtime-model"),
    expire: async () => {
      now += 900001;
    },
    close: async () => {},
  };
});
