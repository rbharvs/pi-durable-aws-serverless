import pg from "pg";
import { check, errorCode } from "../domain.ts";

export interface SqlProvider {
  connect(): Promise<pg.Client>;
  isRetryableAbort(error: unknown): boolean;
}

export function schemaIdentifier(value: string): string {
  check(value.length <= 63 && /^[a-z_][a-z0-9_]*$/.test(value), "Invalid schema identifier");

  return `"${value}"`;
}

export async function connect(config: pg.ClientConfig, schema: string): Promise<pg.Client> {
  const quotedSchema = schemaIdentifier(schema);
  const client = new pg.Client({ connectionTimeoutMillis: 15000, query_timeout: 25000, ...config });

  client.on("error", () => {
    try {
      console.error("pg-socket", "Error");
    } catch {
      // A diagnostic sink must not turn an idle socket error into an uncaught exception.
    }
  });

  try {
    await client.connect();
    await client.query(`SET search_path TO ${quotedSchema}`);

    return client;
  } catch (error) {
    await client.end().catch(() => undefined);

    throw error;
  }
}

export const isPostgresAbort = (e: unknown): boolean =>
  errorCode(e) === "40001" || errorCode(e) === "40P01";

export function postgresProvider(port: number, password: string, schema: string): SqlProvider {
  check(Number.isInteger(port) && port > 0 && port < 65536, "Invalid local port");
  schemaIdentifier(schema);

  return {
    connect: () =>
      connect(
        { host: "127.0.0.1", port, user: "postgres", database: "postgres", password },
        schema,
      ),
    isRetryableAbort: isPostgresAbort,
  };
}
