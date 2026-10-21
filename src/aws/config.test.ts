import { afterEach, expect, it, vi } from "vitest";
import { Readable } from "node:stream";
import pg from "pg";
import { DsqlSigner } from "@aws-sdk/dsql-signer";
import { S3Client } from "@aws-sdk/client-s3";
import { LambdaClient } from "@aws-sdk/client-lambda";
import { aws } from "../../tests/shared/aws.ts";
import { bucketName, region, workerName } from "./config.ts";
import { configuration } from "./common.ts";
import { objectFactory, ImmutableObjects } from "./s3.ts";
import { dsqlSigner, dsqlProvider, isDsqlAbort } from "./dsql.ts";
import { isPostgresAbort } from "../sql/connection.ts";
import { computeSha256, harnessId } from "../domain.ts";
import { dispatcher } from "./dispatch.ts";

const host = "examplecluster.dsql.eu-west-1.on.aws";
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

it("signs offline admin tokens for the explicit host, region, action, and finite expiry", async () => {
  const token = await dsqlSigner(host, aws).getDbConnectAdminAuthToken();
  const url = new URL(`https://${token}`);
  expect(url.hostname).toBe(host);
  expect(url.searchParams.get("Action")).toBe("DbConnectAdmin");
  expect(url.searchParams.get("X-Amz-Credential")).toMatch(
    /^AKIDEXAMPLE\/\d{8}\/eu-west-1\/dsql\/aws4_request$/,
  );
  expect(url.searchParams.get("X-Amz-Expires")).toBe("900");
});

it("validates explicit configuration, resource bounds, host scope, and SQL identifiers before clients", () => {
  for (const value of [undefined, "", "eu-west-1.*", " eu-west-1", "a".repeat(100)]) {
    expect(() => region(value)).toThrow();
  }
  for (const value of [
    "",
    "localhost",
    "https://" + host,
    host + ":5432",
    host + "/",
    "x.dsql.us-east-1.on.aws",
    "user@" + host,
  ]) {
    expect(() => dsqlProvider(value, "service", aws)).toThrow();
  }
  for (const value of ["", "service,public", 'a"; DROP SCHEMA public', "s".repeat(64)]) {
    expect(() => dsqlProvider(host, value, aws)).toThrow();
  }
  for (const value of ["", "ab", "Uppercase", "a..b", "1.2.3.4", "a".repeat(64), "s3://bucket"]) {
    expect(() => bucketName(value)).toThrow();
  }
  for (const value of ["", "x/y", "x".repeat(65), " target", "https://lambda"]) {
    expect(() => workerName(value)).toThrow();
  }
  expect(workerName("service-worker:live")).toBe("service-worker:live");
  expect(
    workerName("arn:aws:lambda:eu-west-1:000000000000:function:service-worker:live"),
  ).toContain(":live");
  expect(bucketName("service.payloads-01")).toBe("service.payloads-01");
  vi.stubEnv("AWS_REGION", "");
  expect(() => configuration()).toThrow("Invalid AWS region");
  vi.stubEnv("DSQL_HOST", "");
  expect(() => configuration(aws)).toThrow("Missing DSQL host");
  vi.stubEnv("DSQL_HOST", host);
  vi.stubEnv("DSQL_SCHEMA", "");
  expect(() => configuration(aws)).toThrow("Missing database schema");
  vi.stubEnv("DSQL_SCHEMA", "service");
  expect(configuration(aws).aws).toBe(aws);
  vi.stubEnv("PAYLOAD_BUCKET", "");
  expect(() => objectFactory(aws)).toThrow();
});

it("DSQL authenticates each connection as admin with verified TLS and bounded pg requests", async () => {
  const sign = vi
    .spyOn(DsqlSigner.prototype, "getDbConnectAdminAuthToken")
    .mockResolvedValue("test-only-token");
  const connect = vi.spyOn(pg.Client.prototype, "connect").mockResolvedValue(undefined);
  const query = vi.spyOn(pg.Client.prototype, "query").mockResolvedValue({ rows: [] } as never);
  const end = vi.spyOn(pg.Client.prototype, "end").mockResolvedValue(undefined);
  const provider = dsqlProvider(host, "service", aws);
  for (let i = 0; i < 2; i++) {
    const client = await provider.connect();
    expect(client).toMatchObject({
      host,
      port: 5432,
      user: "admin",
      database: "postgres",
      ssl: { rejectUnauthorized: true },
    });
    expect(client).toMatchObject({
      _connectionTimeoutMillis: 15000,
      connectionParameters: { query_timeout: 25000 },
    });
    await client.end();
  }
  expect(sign).toHaveBeenCalledTimes(2);
  expect(connect).toHaveBeenCalledTimes(2);
  expect(query.mock.calls).toEqual([
    ['SET search_path TO "service"'],
    ['SET search_path TO "service"'],
  ]);
  expect(end).toHaveBeenCalledTimes(2);
});

it("only DSQL SQLSTATE 40001 proves an abort, not OC000, messages, or connection failures", () => {
  for (const code of ["40001", "40P01", "OC000", "08006", "57P01", 40001, undefined]) {
    expect(isDsqlAbort({ code })).toBe(code === "40001");
    expect(isPostgresAbort({ code })).toBe(code === "40001" || code === "40P01");
  }
  expect(isDsqlAbort(new Error("40001 OC000"))).toBe(false);
});

it("real SDK clients use non-default region, finite transport/retries and static credentials, with no HTTP", async () => {
  vi.stubEnv("AWS_REGION", "ap-southeast-2");
  const json = '{"ok":true}';
  const checksum = Buffer.from(computeSha256(json), "hex").toString("base64");
  const configs: Array<{
    region: string;
    attempts: number;
    timeout: number;
    connection: number;
    throws: boolean;
  }> = [];
  for (const Client of [S3Client, LambdaClient]) {
    vi.spyOn(Client.prototype, "send").mockImplementation(async function (
      this: S3Client | LambdaClient,
    ) {
      const transport = await Reflect.get(this.config.requestHandler, "configProvider");
      configs.push({
        region: await this.config.region(),
        attempts: await this.config.maxAttempts(),
        timeout: transport?.requestTimeout,
        connection: transport?.connectionTimeout,
        throws: transport?.throwOnRequestTimeout,
      });
      expect(await this.config.credentials()).toMatchObject(aws.credentials);
      return { $metadata: {}, ChecksumSHA256: checksum, StatusCode: 202 } as never;
    });
  }
  const objects = new ImmutableObjects("service-payloads", harnessId("config"), aws);
  const dispatch = dispatcher("service-worker", aws, () => {}, "test");
  try {
    await objects.put(json);
    await dispatch.wake(harnessId("config"));
  } finally {
    objects.close();
    dispatch.close();
  }
  expect(configs).toEqual(
    Array(2).fill({
      region: "eu-west-1",
      attempts: 1,
      timeout: 20000,
      connection: 5000,
      throws: true,
    }),
  );
});

it("real S3 serialization and signing stop at an offline HTTP handler", async () => {
  const requests: Array<{ hostname: string; method: string; headers: Record<string, string> }> = [];
  const client = new S3Client({
    ...aws,
    maxAttempts: 1,
    requestHandler: {
      async handle(request: Parameters<S3Client["config"]["requestHandler"]["handle"]>[0]) {
        requests.push(request);
        return {
          response: {
            statusCode: 200,
            headers: { "x-amz-checksum-sha256": request.headers["x-amz-checksum-sha256"] },
            body: Readable.from([]),
          },
        };
      },
    },
  });
  const objects = new ImmutableObjects("service-payloads", harnessId("signed"), aws, client);
  try {
    await objects.put('{"ok":true}');
  } finally {
    objects.close();
  }
  expect(requests).toHaveLength(1);
  expect(requests[0].hostname).toBe("service-payloads.s3.eu-west-1.amazonaws.com");
  expect(requests[0].method).toBe("PUT");
  expect(requests[0].headers["if-none-match"]).toBe("*");
  expect(requests[0].headers.authorization).toContain("/eu-west-1/s3/aws4_request");
});
