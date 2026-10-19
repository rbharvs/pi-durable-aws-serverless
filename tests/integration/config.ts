import { inject } from "vitest";
import { postgresProvider, type SqlProvider } from "../../src/sql/connection.ts";

export type PostgresFixture = { port: number; password: string };

declare module "vitest" {
  export interface ProvidedContext {
    postgres: PostgresFixture;
  }
}

export function configuredProvider(schema = "integration"): SqlProvider {
  const { port, password } = inject("postgres");

  return postgresProvider(port, password, schema);
}
