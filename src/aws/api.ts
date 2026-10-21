import { configuration, logger, type LambdaContext } from "./common.ts";
import { objectFactory } from "./s3.ts";
import { apiHandler, type ApiDependencies } from "../submit.ts";
import { dispatcher } from "./dispatch.ts";
import { workerName } from "./config.ts";
import { cleanup } from "../diagnostics.ts";

/** Core reports handler failures as fixed submit-failed events; composition fails before it. */
function composeFailed(ctx: LambdaContext) {
  try {
    logger("api", ctx)("api-diagnostic", { phase: "compose" });
  } catch {
    // Diagnostics never change the wire result.
  }
}

type Dependencies = Omit<ApiDependencies, "report"> & { close?(): void | Promise<void> };

/** Injection and AWS entry use the same lifecycle. Composition acquires no SQL/object handle. */
export function createHandler(compose: (ctx: LambdaContext) => Dependencies) {
  return async (input: unknown, ctx: LambdaContext) => {
    let deps: Dependencies;
    try {
      deps = compose(ctx);
    } catch {
      composeFailed(ctx);
      return { ok: false as const, code: "internal" as const, retryable: true as const };
    }
    const event: ApiDependencies["event"] = (name, fields) => deps.event(name, fields);
    try {
      // Bind observer methods as well as resource factories across the core boundary.
      return await apiHandler({
        connectAuthority: () => deps.connectAuthority(),
        ...(deps.objects ? { objects: (ns) => deps.objects!(ns) } : {}),
        dispatch: deps.dispatch,
        event,
      })(input);
    } finally {
      if (deps.close) await cleanup({ close: () => deps.close!() }, event, "dispatch");
    }
  };
}

export const handler = createHandler((ctx) => {
  const config = configuration();
  const target = workerName(process.env.WORKER_NAME);
  const objects = objectFactory(config.aws);
  const event = logger("api", ctx);
  // Composition validates the target; the owned client is acquired only on the first wake.
  const dispatch = dispatcher(target, config.aws, event, ctx.awsRequestId);
  return {
    connectAuthority: () => config.connectAuthority(),
    objects,
    event,
    dispatch,
    close: () => dispatch.close(),
  };
});
