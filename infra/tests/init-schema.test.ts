import { afterEach, beforeEach, expect, it, vi } from "vitest";

const database = vi.hoisted(() => ({
  connect: vi.fn(),
  query: vi.fn(),
  end: vi.fn(),
}));
vi.mock("pg", () => ({
  Client: class {
    connect = database.connect;
    query = database.query;
    end = database.end;
  },
}));
const argv = process.argv;
beforeEach(() => {
  vi.resetModules();
  vi.resetAllMocks();
  process.argv = ["node", "init-schema.js", "durable"];
  database.query.mockImplementation(async (sql: string) => {
    if (sql.startsWith("CREATE INDEX ASYNC")) return { rows: [{ job_id: "test-job" }] };
    if (sql === "CALL sys.wait_for_job($1)") return { rows: [{ succeeded: true }] };
    if (sql.includes("sys.wait_for_job")) {
      throw Object.assign(new Error("sys.wait_for_job is a procedure; use CALL"), {
        code: "42809",
      });
    }
    return { rows: [] };
  });
  vi.spyOn(console, "log").mockImplementation(() => {});
});
afterEach(() => {
  process.argv = argv;
  vi.restoreAllMocks();
});

it("runs the reference tables and async indexes, waiting after each index", async () => {
  await import("../src/init-schema.ts");
  const sql = database.query.mock.calls.map(([statement]) => statement as string);
  expect(sql[0]).toBe('CREATE SCHEMA "durable"');
  expect(sql.filter((statement) => statement.startsWith("CREATE TABLE"))).toHaveLength(6);
  expect(sql.filter((statement) => statement.startsWith("CREATE INDEX ASYNC"))).toHaveLength(9);
  expect(sql.filter((statement) => statement.includes("CHECK"))).toHaveLength(4);
  expect(sql).toContain(
    "CREATE INDEX ASYNC inbox_unmapped ON inbox(ns,received_at,request_id) WHERE submission_id IS NULL",
  );
  for (const [index, statement] of sql.entries()) {
    if (statement.startsWith("CREATE INDEX ASYNC")) {
      expect(database.query.mock.calls[index + 1]).toEqual([
        "CALL sys.wait_for_job($1)",
        ["test-job"],
      ]);
    }
  }
  expect(console.log).toHaveBeenCalledWith('Completed: CREATE SCHEMA "durable"');
  expect(console.log).toHaveBeenCalledWith(
    "Completed: Statement 15/15: CREATE INDEX ASYNC inbox_unmapped ON inbox(ns,received_at,request_id) WHERE submission_id IS NULL",
  );
  expect(database.end).toHaveBeenCalledOnce();
});
it.each([false, null, undefined])(
  "stops when the procedure reports succeeded=%s",
  async (succeeded) => {
    database.query.mockImplementation(async (sql: string) => {
      if (sql.startsWith("CREATE INDEX ASYNC")) return { rows: [{ job_id: "failed-job" }] };
      if (sql === "CALL sys.wait_for_job($1)") {
        return { rows: succeeded === undefined ? [] : [{ succeeded }] };
      }
      return { rows: [] };
    });
    await expect(import("../src/init-schema.ts")).rejects.toMatchObject({
      message: expect.stringContaining("Statement 4/15: CREATE INDEX ASYNC records_type"),
      cause: expect.objectContaining({ message: "Index job did not report success: failed-job" }),
    });
    expect(console.log).not.toHaveBeenCalledWith(
      expect.stringContaining("Completed: Statement 4/15"),
    );
    expect(
      database.query.mock.calls.filter(([sql]) => sql.startsWith("CREATE INDEX ASYNC")),
    ).toHaveLength(1);
    expect(database.end).toHaveBeenCalledOnce();
  },
);
it("preserves a procedure query error, stops subsequent DDL and closes the connection", async () => {
  const cause = new Error("simulated wait query timeout");
  const query = database.query.getMockImplementation()!;
  database.query.mockImplementation(async (sql: string) => {
    if (sql === "CALL sys.wait_for_job($1)") throw cause;
    return query(sql);
  });
  await expect(import("../src/init-schema.ts")).rejects.toMatchObject({
    message: expect.stringContaining("Statement 4/15: CREATE INDEX ASYNC records_type"),
    cause,
  });
  expect(database.query.mock.calls.at(-1)).toEqual(["CALL sys.wait_for_job($1)", ["test-job"]]);
  expect(console.log).not.toHaveBeenCalledWith(
    expect.stringContaining("Completed: Statement 4/15"),
  );
  expect(database.end).toHaveBeenCalledOnce();
});
it("identifies a failed DDL statement, preserves its cause and reports completed work", async () => {
  const cause = new Error("simulated server error");
  database.query.mockImplementation(async (sql: string) => {
    if (sql.startsWith("CREATE TABLE records")) throw cause;
    return { rows: [] };
  });
  await expect(import("../src/init-schema.ts")).rejects.toMatchObject({
    message: expect.stringMatching(
      /Statement 2\/15: CREATE TABLE records.*partial schema; do not submit API requests; inspect completed statements/,
    ),
    cause,
  });
  expect(console.log).toHaveBeenCalledWith("Completed: Statement 1/15: CREATE TABLE harness (");
  expect(console.log).not.toHaveBeenCalledWith(
    expect.stringContaining("Completed: Statement 2/15"),
  );
  expect(database.query.mock.calls).toHaveLength(4);
  expect(database.end).toHaveBeenCalledOnce();
});
it("does not replay DDL over an existing schema", async () => {
  database.query.mockRejectedValue(new Error("schema already exists"));
  await expect(import("../src/init-schema.ts")).rejects.toMatchObject({
    message: expect.stringContaining('CREATE SCHEMA "durable" failed'),
    cause: expect.objectContaining({ message: "schema already exists" }),
  });
  expect(database.query.mock.calls).toEqual([['CREATE SCHEMA "durable"']]);
  expect(database.end).toHaveBeenCalledOnce();
});
