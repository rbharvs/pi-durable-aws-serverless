import { afterEach, expect, it, vi } from "vitest";
import fs from "node:fs";
import net from "node:net";
import http from "node:http";
import https from "node:https";
import pg from "pg";
import { configuration } from "./common.ts";

// A separate test file gets an isolated SDK module cache. aws-setup clears ambient AWS
// variables first; install these invented Lambda credentials only after that setup.
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

it("the default Lambda root signs with the real SDK environment chain before pg setup", async () => {
  const forbidden = vi.fn((): never => {
    throw new Error("Unexpected credential-file or network I/O");
  });
  vi.spyOn(fs, "readFileSync").mockImplementation(forbidden);
  vi.spyOn(fs, "readFile").mockImplementation(forbidden);
  vi.spyOn(fs.promises, "readFile").mockImplementation(forbidden);
  vi.spyOn(net.Socket.prototype, "connect").mockImplementation(forbidden);
  vi.spyOn(http, "request").mockImplementation(forbidden);
  vi.spyOn(https, "request").mockImplementation(forbidden);
  vi.stubGlobal("fetch", forbidden);

  const host = "examplecluster.dsql.eu-west-1.on.aws";
  vi.stubEnv("AWS_LAMBDA_FUNCTION_NAME", "offline-signer-regression");
  vi.stubEnv("AWS_REGION", "eu-west-1");
  vi.stubEnv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE");
  vi.stubEnv("AWS_SECRET_ACCESS_KEY", "offline-test-only-secret");
  vi.stubEnv("AWS_SESSION_TOKEN", "offline-test-only-session");
  vi.stubEnv("DSQL_HOST", host);
  vi.stubEnv("DSQL_SCHEMA", "service");

  const connect = vi.spyOn(pg.Client.prototype, "connect").mockResolvedValue(undefined);
  const query = vi.spyOn(pg.Client.prototype, "query").mockResolvedValue({ rows: [] } as never);
  const end = vi.spyOn(pg.Client.prototype, "end").mockResolvedValue(undefined);
  try {
    // No injected credentials and no signer mock: exercise the deployed root's defaults.
    const client = await configuration().provider.connect();
    try {
      const url = new URL(`https://${client.password}`);
      expect(url.hostname).toBe(host);
      expect(url.searchParams.get("Action")).toBe("DbConnectAdmin");
      expect(url.searchParams.get("X-Amz-Credential")).toMatch(
        /^AKIDEXAMPLE\/\d{8}\/eu-west-1\/dsql\/aws4_request$/,
      );
      expect(url.searchParams.get("X-Amz-Expires")).toBe("900");
      expect(url.searchParams.get("X-Amz-Security-Token")).toBe("offline-test-only-session");
      expect(url.searchParams.get("X-Amz-Signature")).toMatch(/^[a-f0-9]{64}$/);
      expect(connect).toHaveBeenCalledTimes(1);
      expect(query.mock.calls).toEqual([['SET search_path TO "service"']]);
    } finally {
      await client.end();
    }
    expect(end).toHaveBeenCalledTimes(1);
    expect(forbidden).not.toHaveBeenCalled();
  } finally {
    vi.unstubAllGlobals();
  }
});
