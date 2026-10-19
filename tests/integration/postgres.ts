import { randomUUID } from "node:crypto";
import type pg from "pg";
import { Database, type Row, type SqlValue } from "../../src/sql/db.ts";
import { harnessId } from "../../src/domain.ts";
import { configuredProvider } from "./config.ts";
import { Control } from "./authority.ts";

export const provider = configuredProvider();

export type QueryHook = (
  phase: "before" | "after",
  sql: string,
  params: readonly SqlValue[],
  rows?: Row[],
) => Promise<void>;

export type Query = {
  sql: string;
  params: readonly SqlValue[];
  acknowledged: boolean;
  error?: unknown;
};

export function wrap(client: pg.Client, hook: QueryHook, trace: Query[] = []): pg.Client {
  return new Proxy(client, {
    get(target, prop) {
      if (prop === "query")
        return async (sql: string, params: SqlValue[] = []) => {
          const query: Query = { sql, params: structuredClone(params), acknowledged: false };

          trace.push(query);

          try {
            await hook("before", sql, params);

            const result = await target.query<Row>(sql, params);

            await hook("after", sql, params, result.rows);
            query.acknowledged = true;

            return result;
          } catch (error) {
            query.error = error;

            throw error;
          }
        };

      const value: unknown = Reflect.get(target, prop);

      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

export const abort = () => Object.assign(new Error("confirmed statement abort"), { code: "40001" });

/** Fires one injected fault at the first matching statement boundary. */
export function once(phase: "before" | "after", prefix: string, fault: () => unknown): QueryHook {
  let fired = false;

  return async (at, sql) => {
    if (fired || at !== phase || !sql.startsWith(prefix)) return;
    fired = true;

    throw await fault();
  };
}

/** An authority over its own traced connection; the hook can inject faults at statement boundaries. */
export async function traced(hook: QueryHook = async () => {}, trace: Query[] = []) {
  const control = new Control(new Database(wrap(await provider.connect(), hook, trace)), provider);

  return { control, trace };
}

/** A unique namespace that is not created in advance; ingress must create its head itself. */
export async function namespace(label: string) {
  const ns = harnessId(`test_${label}_${randomUUID().replaceAll("-", "")}`),
    control = new Control(await Database.connect(provider), provider);

  return {
    ns,
    control,

    async inbox(): Promise<Row[]> {
      return control.inspectionDb.query("SELECT * FROM inbox WHERE ns=$1 ORDER BY request_id", [
        ns,
      ]);
    },

    async heads(): Promise<Row[]> {
      return control.inspectionDb.query("SELECT * FROM harness WHERE ns=$1", [ns]);
    },

    async finish() {
      try {
        for (const table of ["inbox", "harness"])
          await control.inspectionDb.query(`DELETE FROM ${table} WHERE starts_with(ns, $1)`, [ns]);
      } finally {
        await control.close();
      }
    },
  };
}

export const statements = (trace: readonly Query[], prefix: string) =>
  trace.filter((q) => q.sql.startsWith(prefix));
