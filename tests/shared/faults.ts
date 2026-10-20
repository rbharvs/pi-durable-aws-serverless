import type { Storage } from "@earendil-works/pi-durable";

/** Transport uncertainty decorator at an operation boundary, never a runtime flag. */
export function loseAcknowledgement(
  storage: Storage,
  operation: "mintId" | "commit",
  applied: boolean,
): Storage {
  let fired = false,
    failed = false;

  return new Proxy(storage, {
    get(target, key) {
      if (key === "then") return undefined;
      if (key === "close") return target.close.bind(target);

      return async (...args: unknown[]) => {
        if (failed) throw new Error("Storage transport handle failed");

        if (key === operation && !fired) {
          fired = true;
          failed = true;
          if (applied) await Reflect.apply(Reflect.get(target, key), target, args);

          throw new Error("Operation outcome unknown");
        }

        return Reflect.apply(Reflect.get(target, key), target, args);
      };
    },
  });
}
