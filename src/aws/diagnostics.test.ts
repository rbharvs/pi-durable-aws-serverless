import { afterEach, expect, it, vi } from "vitest";
import pg from "pg";
import { createHandler } from "./worker.ts";
import { logger } from "./common.ts";
import { createMemoryBackend } from "../memory.ts";
import { dsqlProvider } from "./dsql.ts";
import { aws, lambdaContext } from "../../tests/shared/aws.ts";

afterEach(() => vi.restoreAllMocks());

it("worker root does not serialize a provider's arbitrary error name", async () => {
  const backend = createMemoryBackend();
  const error = Object.assign(Error("private SQL password"), { name: "private-provider-token" });
  const log = vi.spyOn(console, "log").mockImplementation(() => {});
  const handler = createHandler(() => ({
    async connectAuthority() {
      const a = await backend.connectAuthority();
      a.claim = async () => {
        throw error;
      };
      return a;
    },
    async open() {
      throw Error("must not open");
    },
    async resolve() {
      throw Error("must not resolve");
    },
    event: logger("worker", lambdaContext),
  }));
  await expect(handler({ ns: "diagnostics" }, lambdaContext)).rejects.toBe(error);
  expect(log.mock.calls.map(([row]) => JSON.parse(row))).toEqual([
    {
      source: "worker",
      invocation: lambdaContext.awsRequestId,
      at: expect.any(Number),
      event: "worker-error",
      ns: "diagnostics",
      cause: "Error",
    },
  ]);
  expect(JSON.stringify(log.mock.calls)).not.toContain("private");
});

it("DSQL socket diagnostics use a fixed value and cannot throw from a broken logger", async () => {
  vi.spyOn(pg.Client.prototype, "connect").mockResolvedValue(undefined);
  vi.spyOn(pg.Client.prototype, "query").mockResolvedValue({ rows: [] } as never);
  vi.spyOn(pg.Client.prototype, "end").mockResolvedValue(undefined);
  const log = vi.spyOn(console, "error").mockImplementation(() => {});
  const client = await dsqlProvider(
    "examplecluster.dsql.eu-west-1.on.aws",
    "service",
    aws,
  ).connect();
  try {
    const error = Object.assign(Error("private password"), { name: "private-token" });
    client.emit("error", error);
    expect(log.mock.calls).toEqual([["pg-socket", "Error"]]);
    log.mockImplementation(() => {
      throw Error("broken logger");
    });
    expect(() => client.emit("error", error)).not.toThrow();
  } finally {
    await client.end();
  }
});
