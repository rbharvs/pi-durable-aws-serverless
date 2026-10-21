import { expect, it } from "vitest";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import * as publicApi from "./index.ts";
import { roots, bundleGraph, assertGraphBoundaries, type Root } from "../graphs.ts";

it("public runtime exports remain deliberate and provider-independent", () => {
  expect(Object.keys(publicApi).sort()).toEqual([
    "ObjectInvalid",
    "apiHandler",
    "canonical",
    "createMemoryBackend",
    "harnessId",
    "parseCommand",
    "publishedPayload",
    "reconcile",
    "requestId",
    "runtimeDependencies",
    "stagedReference",
    "worker",
  ]);
});

for (const root of Object.keys(roots) as Root[])
  it(`normal ${root} output excludes its forbidden dependencies`, async () => {
    const graph = await bundleGraph(root);
    expect(() => assertGraphBoundaries(root, graph)).not.toThrow();
  });

async function fixture(root: Root, source: string) {
  const directory = await mkdtemp(join(tmpdir(), "durable-graph-"));
  try {
    const file = join(directory, "entry.ts");
    await writeFile(file, source);
    return await bundleGraph(root, file);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
const from = (path: string) => JSON.stringify(resolve(path));

const forbidden: readonly [Root, string, string][] = [
  ["worker", "project dispatcher", `export { dispatcher } from ${from("src/aws/dispatch.ts")};`],
  ["api", "local runtime", `export { runtimeDependencies } from ${from("src/runtime.ts")};`],
  ["api", "SQL storage", `export { SqlStorage } from ${from("src/sql/storage.ts")};`],
  [
    "reconciler",
    "Pi engine",
    `export { Harness } from ${from("node_modules/@earendil-works/pi-durable")};`,
  ],
  ["reconciler", "S3 adapter", `export { ImmutableObjects } from ${from("src/aws/s3.ts")};`],
  [
    "reconciler",
    "S3 client",
    `export { S3Client } from ${from("node_modules/@aws-sdk/client-s3")};`,
  ],
  ["index", "SQL adapter", `export { postgresProvider } from ${from("src/sql/connection.ts")};`],
  ["index", "SQL client", `export { default as pg } from ${from("node_modules/pg")};`],
  ["index", "AWS adapter", `export { dispatcher } from ${from("src/aws/dispatch.ts")};`],
  [
    "index",
    "AWS client",
    `export { LambdaClient } from ${from("node_modules/@aws-sdk/client-lambda")};`,
  ],
];
for (const [root, label, source] of forbidden)
  it(`${root} rejects a real emitted ${label} import`, async () => {
    const graph = await fixture(root, source);
    expect(() => assertGraphBoundaries(root, graph)).toThrow(`Forbidden ${root} dependencies`);
  });

for (const [root, path, symbol] of [
  ["worker", "src/aws/dispatch.ts", "dispatcher"],
  ["api", "src/runtime.ts", "runtimeDependencies"],
] as const)
  it(`${root} allows parsed but eliminated ${symbol} imports`, async () => {
    const graph = await fixture(
      root,
      `
      import { ${symbol} } from ${from(path)};
      const unused = () => ${symbol};
      export const handler = () => "InvokeCommand";
    `,
    );
    expect(Object.keys(graph.metafile.inputs)).toContain(path);
    expect(() => assertGraphBoundaries(root, graph)).not.toThrow();
  });

it("a worker may include an operator Lambda tool and harmless invocation strings", async () => {
  const graph = await fixture(
    "worker",
    `
    export { handler } from ${from("tests/fixtures/aws-worker.ts")};
    import { LambdaClient, InvokeCommand } from ${from("node_modules/@aws-sdk/client-lambda")};
    export const tool = (client: LambdaClient) => client.send(new InvokeCommand({ FunctionName: "other" }));
    export const description = "InvokeCommand dispatch-send";
  `,
  );
  expect(
    Object.values(graph.metafile.outputs).some((output) =>
      Object.entries(output.inputs).some(
        ([path, input]) => path.includes("@aws-sdk/client-lambda/") && input.bytesInOutput > 0,
      ),
    ),
  ).toBe(true);
  expect(() => assertGraphBoundaries("worker", graph)).not.toThrow();
});
