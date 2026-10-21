import { configuration, logger, type LambdaContext } from "./common.ts";
import { reconcile } from "../reconciler.ts";
import { dispatcher } from "./dispatch.ts";
import { workerName } from "./config.ts";
import { cleanup } from "../diagnostics.ts";

type Dependencies = Parameters<typeof reconcile>[0] & { close?(): void | Promise<void> };

export function createHandler(compose: (ctx: LambdaContext) => Dependencies) {
  return async (_input: unknown, ctx: LambdaContext) => {
    const deps = compose(ctx);
    try {
      // Core reconcile calls dependency methods on deps, preserving their receivers.
      return await reconcile(deps);
    } finally {
      if (deps.close) {
        await cleanup(
          { close: () => deps.close!() },
          (name, fields) => {
            deps.event(name, fields);
          },
          "dispatch",
        );
      }
    }
  };
}

export const handler = createHandler((ctx) => {
  const config = configuration();
  const target = workerName(process.env.WORKER_NAME);
  const event = logger("reconciler", ctx);
  const dispatch = dispatcher(target, config.aws, event, ctx.awsRequestId);
  return {
    connectAuthority: () => config.connectAuthority(),
    event,
    dispatch,
    close: () => dispatch.close(),
  };
});
