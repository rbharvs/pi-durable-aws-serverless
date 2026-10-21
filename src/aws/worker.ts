import { configuration, logger, type LambdaContext } from "./common.ts";
import { objectFactory } from "./s3.ts";
import { createSqlBackend } from "../sql/backend.ts";
import { runtimeDependencies } from "../runtime.ts";
import { worker, type WorkerDependencies } from "../worker.ts";
import { harnessId, object } from "../domain.ts";
import type { HarnessOptions, AgentChange } from "@earendil-works/pi-durable";

export function createHandler(compose: (ctx: LambdaContext) => WorkerDependencies) {
  return async (input: unknown, ctx: LambdaContext) => {
    const ns = harnessId(object(input).ns);
    const deps = compose(ctx);
    return worker(ns, () => ctx.getRemainingTimeInMillis(), deps);
  };
}

/** No default model, registry, credentials, or dummy handler. The deployer owns the real entry. */
export function createAwsHandler(options: HarnessOptions, rootAgent: AgentChange) {
  return createHandler((ctx) => {
    const { provider, aws } = configuration();
    const objects = objectFactory(aws);
    return runtimeDependencies({
      backend: createSqlBackend(provider, objects),
      harnessOptions: options,
      objects,
      event: logger("worker", ctx),
      rootAgent,
    });
  });
}
