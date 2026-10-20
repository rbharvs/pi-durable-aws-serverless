import { randomUUID } from "node:crypto";
import type pg from "pg";
import { Database, type Row, type SqlValue } from "../../src/sql/db.ts";
import { harnessId } from "../../src/domain.ts";
import { configuredProvider } from "./config.ts";
import { InspectableAuthority } from "./authority.ts";

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

/** A serialization failure (SQLSTATE 40001), which the provider confirms committed nothing. */
export const confirmedAbort = () =>
  Object.assign(new Error("confirmed statement abort"), { code: "40001" });

/** Fires one injected fault at the first matching statement boundary. */
export function faultOnce(
  phase: "before" | "after",
  prefix: string,
  fault: () => unknown,
): QueryHook {
  let fired = false;

  return async (at, sql) => {
    if (fired || at !== phase || !sql.startsWith(prefix)) return;
    fired = true;

    throw await fault();
  };
}

/** An authority over its own traced connection; the hook can inject faults at statement boundaries. */
export async function traced(hook: QueryHook = async () => {}, trace: Query[] = []) {
  const authority = new InspectableAuthority(
    new Database(wrap(await provider.connect(), hook, trace)),
    provider,
  );

  return { authority, trace };
}

/** One traced connection; close-count inspection is fixture-only, never a storage port. */
export async function historyConnection(hook: QueryHook = async () => {}) {
  const trace: Query[] = [];
  const client = wrap(await provider.connect(), hook, trace);
  let ends = 0;
  const db = new Database({
    query: client.query.bind(client),
    async end() {
      ends++;
      await client.end();
    },
  });

  return { db, trace, endCalls: () => ends };
}

/** An isolated namespace with its own authority; `finish()` deletes its rows and closes it. */
export async function namespace(label: string) {
  const ns = harnessId(`test_${label}_${randomUUID().replaceAll("-", "")}`);
  const authority = new InspectableAuthority(await Database.connect(provider), provider);

  return {
    ns,
    authority,

    async inbox(): Promise<Row[]> {
      return authority.inspectionDb.query("SELECT * FROM inbox WHERE ns=$1 ORDER BY request_id", [
        ns,
      ]);
    },

    async finish() {
      try {
        // Fixed table names and the full random namespace prefix: never clean another test's rows.
        for (const table of ["inbox", "records", "inline_payloads", "s3_payloads", "harness"])
          await authority.inspectionDb.query(`DELETE FROM ${table} WHERE starts_with(ns, $1)`, [
            ns,
          ]);
      } finally {
        await authority.close();
      }
    },
  };
}

export const statements = (trace: readonly Query[], prefix: string) =>
  trace.filter((q) => q.sql.startsWith(prefix));
