import type { Backend } from "../authority.ts";
import type { HarnessId } from "../domain.ts";
import type { PayloadObjects } from "../s3-payload.ts";
import type { SqlProvider } from "./connection.ts";
import { SqlAuthority } from "./authority.ts";
import { Database } from "./db.ts";
import { SqlStorage } from "./storage.ts";

/** Each acquired object handle and connection has exactly one close, after joined operations. */
export function createSqlBackend(
  provider: SqlProvider,
  objects: (ns: HarnessId) => PayloadObjects & { close(): void | Promise<void> },
): Backend {
  return {
    connectAuthority: async () => new SqlAuthority(await Database.connect(provider), provider),
    async openStorage(lease) {
      const store = objects(lease.ns);
      let db: Database | undefined;
      try {
        db = await Database.connect(provider);
        const storage = await SqlStorage.open(db, lease, provider, store);
        const close = storage.close.bind(storage);
        let closing: Promise<void> | undefined;
        storage.close = (context) =>
          (closing ??= (async () => {
            const errors: unknown[] = [];
            try {
              await close(context);
            } catch (error) {
              errors.push(error);
            }
            try {
              await store.close();
            } catch (error) {
              errors.push(error);
            }
            if (errors.length === 1) throw errors[0];
            if (errors.length > 1) {
              throw new AggregateError(errors, "Storage cleanup failed", { cause: errors[0] });
            }
          })());
        return storage;
      } catch (error) {
        const errors: unknown[] = [error];
        try {
          await db?.end();
        } catch (failure) {
          errors.push(failure);
        }
        try {
          await store.close();
        } catch (failure) {
          errors.push(failure);
        }
        if (errors.length > 1) {
          throw new AggregateError(errors, "Storage acquisition and cleanup failed", {
            cause: error,
          });
        }
        throw error;
      }
    },
  };
}
