import { it, expect } from "vitest";
import { Database } from "../../src/sql/db.ts";
import { DB_NOW_MS } from "../../src/sql/authority.ts";
import { provider } from "./postgres.ts";
import { configuredProvider } from "./config.ts";
import { integer } from "../../src/domain.ts";

it("uses C ordering and database millisecond units", async () => {
  const db = await Database.connect(provider);

  try {
    const rows = await db.query(
      "SELECT k FROM (VALUES ('ä'),('z'),('A'),('Z'),('a'),('😀')) AS keys(k) ORDER BY k",
    );

    expect(rows.map((r) => r.k)).toEqual(["A", "Z", "a", "z", "ä", "😀"]);

    const clock = await db.query(`SELECT ${DB_NOW_MS} AS ms`);

    expect(Math.abs(integer(clock[0].ms) - Date.now())).toBeLessThan(10000);
  } finally {
    await db.end();
  }
});

it("uses the full 63-character schema name on PostgreSQL without truncation or equal-prefix aliasing", async () => {
  const schema = "s".repeat(63),
    client = await configuredProvider(schema).connect();

  try {
    await client.query(`CREATE SCHEMA "${schema}"`);
    expect((await client.query("SELECT current_schema() AS schema")).rows[0].schema).toBe(schema);
    for (const suffix of ["a", "b"])
      expect(() => configuredProvider(schema + suffix)).toThrow("Invalid schema identifier");
  } finally {
    try {
      await client.query(`DROP SCHEMA "${schema}" CASCADE`);
    } finally {
      await client.end();
    }
  }
});
