import { randomUUID } from "node:crypto";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { region, workerName, REQUEST_MS, transport, type AwsOptions } from "./config.ts";
import { check, harnessId, type HarnessId } from "../domain.ts";
import type { Dispatch, Events } from "../dispatch.ts";
import { emit } from "../diagnostics.ts";

/** API/reconciler only. An accepted asynchronous hint is not completed work. */
export function dispatcher(
  functionName: string,
  options: AwsOptions,
  event: Events,
  parentInvocation: string,
  injected?: Pick<LambdaClient, "send" | "destroy">,
): Dispatch & { close(): void } {
  workerName(functionName);
  const configuredRegion = region(options.region);
  check(parentInvocation.length > 0 && parentInvocation.length <= 128, "Invalid invocation ID");
  // An injected client is already acquired and owned. Otherwise acquire only on first wake.
  let client = injected;
  const active = new Set<AbortController>();
  let closed = false;
  return {
    async wake(ns: HarnessId) {
      check(!closed, "Dispatcher closed");
      harnessId(ns);
      const dispatchId = randomUUID();
      // Synchronous acquisition before any await/observer: concurrent wakes share one client.
      const sender = (client ??= new LambdaClient({
        region: configuredRegion,
        credentials: options.credentials,
        maxAttempts: 1,
        requestHandler: transport,
      }));
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), REQUEST_MS);
      timer.unref();
      active.add(controller);
      try {
        emit(event, "dispatch-send", { ns, target: functionName, dispatchId, parentInvocation });
        const response = await sender.send(
          new InvokeCommand({
            FunctionName: functionName,
            InvocationType: "Event",
            Payload: Buffer.from(JSON.stringify({ ns, dispatchId, parentInvocation })),
          }),
          { abortSignal: controller.signal },
        );
        check(response.StatusCode === 202 && !response.FunctionError, "Lambda hint not accepted");
        emit(event, "dispatch-accepted", {
          ns,
          dispatchId,
          parentInvocation,
          requestId: response.$metadata.requestId,
        });
      } finally {
        clearTimeout(timer);
        active.delete(controller);
      }
    },
    close() {
      if (closed) return;
      closed = true;
      for (const controller of active) controller.abort();
      client?.destroy();
    },
  };
}
