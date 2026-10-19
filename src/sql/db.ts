import type pg from "pg";
import type { SqlProvider } from "./connection.ts";

export type SqlValue = string | number | boolean | null;

export type Row = Record<string, unknown>;

export type Connection = Pick<pg.Client, "query" | "end">;

export class Database {
  constructor(private readonly client: Connection) {}

  static async connect(provider: SqlProvider): Promise<Database> {
    return new Database(await provider.connect());
  }

  async query(sql: string, params: readonly SqlValue[] = []): Promise<Row[]> {
    return (await this.client.query<Row>(sql, [...params])).rows;
  }

  async end(): Promise<void> {
    await this.client.end();
  }
}
