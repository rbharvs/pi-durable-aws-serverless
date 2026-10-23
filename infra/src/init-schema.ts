import { readFileSync } from "node:fs";
import { Client } from "pg";

const [schema, ...extra] = process.argv.slice(2);
if (!schema || extra.length) throw new Error("Usage: node infra/dist/init-schema.js <schema>");
const source = readFileSync(new URL("../../src/sql/schema.sql", import.meta.url), "utf8");
// The current reference DDL has line comments and semicolon-delimited statements,
// with no semicolons inside literals. This is a one-shot bootstrap, not a migration tool.
const statements = source
  .replace(/^--.*$/gm, "")
  .split(";")
  .map((sql) => sql.trim())
  .filter(Boolean);
const client = new Client({
  ssl: { rejectUnauthorized: true },
  connectionTimeoutMillis: 15000,
  query_timeout: 600000,
}); // Uses standard PGHOST, PGUSER, PGDATABASE and PGPASSWORD environment variables.
let current = "Connect";
try {
  await client.connect();
  const identifier = `"${schema.replaceAll('"', '""')}"`;
  current = `CREATE SCHEMA ${identifier}`;
  await client.query(current); // Fails if the schema already exists.
  console.log(`Completed: ${current}`);
  current = `SET search_path TO ${identifier}`;
  await client.query(current);
  for (const [index, statement] of statements.entries()) {
    const sql = statement.replace(/^CREATE INDEX /, "CREATE INDEX ASYNC ");
    current = `Statement ${index + 1}/${statements.length}: ${sql.split("\n")[0]}`;
    const result = await client.query(sql);
    if (sql.startsWith("CREATE INDEX ASYNC ")) {
      const job = result.rows[0]?.job_id;
      if (typeof job !== "string") throw new Error("DSQL did not return an index job_id");
      const waited = await client.query("CALL sys.wait_for_job($1)", [job]);
      if (waited.rows[0]?.succeeded !== true) {
        throw new Error(`Index job did not report success: ${job}`);
      }
    }
    console.log(`Completed: ${current}`);
  }
  console.log("Schema created; all index jobs succeeded. Review before submitting API requests.");
} catch (cause) {
  throw new Error(
    `${current} failed; possible partial schema; do not submit API requests; inspect completed statements.`,
    { cause },
  );
} finally {
  await client.end();
}
