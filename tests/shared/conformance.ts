import { describe, it, expect } from "vitest";
import type { Storage } from "@earendil-works/pi-durable";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context";

export function registerConformance(
  name: string,
  make: () => Promise<{ openStorage(): Promise<Storage>; finish(): Promise<void> }>,
) {
  for (const reopen of [false, true])
    registerStorageConformance(
      { describe, it, expect },
      `${name} upstream${reopen ? "/reopen-every-commit" : "/live"}`,
      async (use) => {
        const f = await make();
        let storage: Storage | undefined;
        try {
          let current = await f.openStorage();
          let closed = false;
          storage = !reopen
            ? current
            : new Proxy<Storage>(current, {
                get(_target, key) {
                  if (key === "then") return undefined;
                  if (key === "close")
                    return async () => {
                      closed = true;
                      await current.close(context);
                    };
                  if (closed) return () => Promise.reject(Error("Storage wrapper closed"));
                  if (key === "commit")
                    return async (...args: Parameters<Storage["commit"]>) => {
                      try {
                        return await current.commit(...args);
                      } finally {
                        await current.close(context);
                        if (!closed) current = await f.openStorage();
                      }
                    };
                  return (...args: unknown[]) =>
                    Reflect.apply(Reflect.get(current, key), current, args);
                },
              });
          await use(storage);
        } finally {
          await storage?.close(context);
          await f.finish();
        }
      },
    );
}
