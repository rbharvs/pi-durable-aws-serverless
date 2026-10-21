import { expect, it } from "vitest";
import {
  cp,
  link,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { bundleGraph, assertGraphBoundaries, roots } from "../graphs.ts";

async function dependencyMetadata() {
  return Promise.all(
    [".modules.yaml", ".pnpm/lock.yaml", ".pnpm-workspace-state-v1.json"].map(async (path) =>
      createHash("sha256")
        .update(await readFile(resolve("node_modules", path)))
        .digest("hex"),
    ),
  );
}

/** Run the real package CLI in an owned source copy, never over the developer's dist. */
async function project(run: (directory: string) => Promise<void>) {
  const before = await dependencyMetadata();
  const directory = await mkdtemp(join(tmpdir(), "durable-bundle-"));
  try {
    for (const path of [
      "src",
      "tests/fixtures",
      "build.mjs",
      "graphs.ts",
      "package.json",
      "pnpm-lock.yaml",
      "tsconfig.json",
    ]) {
      await cp(resolve(path), join(directory, path), { recursive: true });
    }
    await symlink(resolve("node_modules"), join(directory, "node_modules"), "dir");
    await run(directory);
  } finally {
    await rm(directory, { recursive: true, force: true });
    expect(await dependencyMetadata()).toEqual(before);
  }
}
function cli(directory: string, entry?: string) {
  // The temporary copy reuses installed dependencies; never install through its shared symlink.
  const result = spawnSync(
    "pnpm",
    ["--config.verify-deps-before-run=false", "build", ...(entry ? ["--", entry] : [])],
    {
      cwd: directory,
      encoding: "utf8",
      timeout: 30000,
    },
  );
  if (result.error) throw result.error;
  const output = result.stdout + result.stderr;
  expect(output).not.toMatch(
    /supply-chain|installing dependencies|install that runs before scripts|verifyDepsBeforeRun|\[WARN\]/i,
  );
  return { status: result.status, output };
}
const files = ["api-meta.json", "api.cjs", "reconciler-meta.json", "reconciler.cjs"];

it("the build CLI writes the checked configured worker bytes and clears only stale worker outputs", async () => {
  await project(async (directory) => {
    expect(cli(directory)).toMatchObject({ status: 0 });
    expect(await readdir(join(directory, "dist"))).toEqual(files);
    await mkdir(join(directory, "operator"));
    const entry = "operator/entry.ts";
    await writeFile(
      join(directory, entry),
      'export { handler } from "../tests/fixtures/aws-worker.ts";',
    );
    expect(cli(directory, entry)).toMatchObject({ status: 0 });
    for (const name of ["api", "reconciler", "worker"] as const) {
      const graph = await bundleGraph(name, name === "worker" ? entry : roots[name], directory);
      assertGraphBoundaries(name, graph);
      const artifact = join(directory, `dist/${name}.cjs`);
      const bytes = await readFile(artifact);
      expect(bytes.equals(Buffer.from(graph.outputFiles![0].contents))).toBe(true);
      const metadata = JSON.parse(
        await readFile(join(directory, `dist/${name}-meta.json`), "utf8"),
      );
      expect(metadata).toEqual(graph.metafile);
      expect(Object.keys(metadata.outputs)).toEqual([`dist/${name}.cjs`]);
      expect(metadata.outputs[`dist/${name}.cjs`].bytes).toBe(bytes.length);
      // Only controlled repository fixtures are loaded; normal builds never evaluate entries.
      const exported = createRequire(import.meta.url)(artifact);
      expect(typeof exported.handler).toBe("function");
      if (name === "worker") {
        expect(Object.keys(exported)).toEqual(["handler"]);
        await expect(
          exported.handler(
            { ns: "invalid/path" },
            {
              getRemainingTimeInMillis: () => 45000,
            },
          ),
        ).rejects.toThrow();
      }
    }
    await writeFile(join(directory, "dist/keep.txt"), "not build-owned");
    expect(cli(directory)).toMatchObject({ status: 0 });
    expect((await readdir(join(directory, "dist"))).sort()).toEqual([...files, "keep.txt"].sort());
    expect(await readFile(join(directory, "dist/keep.txt"), "utf8")).toBe("not build-owned");
  });
});

it("the build CLI rejects an emitted project dispatcher before writing artifacts", async () => {
  await project(async (directory) => {
    await writeFile(
      join(directory, "operator.ts"),
      [
        'export { handler } from "./tests/fixtures/aws-worker.ts";',
        'export { dispatcher } from "./src/aws/dispatch.ts";',
      ].join("\n"),
    );
    const result = cli(directory, "operator.ts");
    expect(result.status).not.toBe(0);
    expect(result.output).toContain("Forbidden worker dependencies");
    await expect(lstat(join(directory, "dist"))).rejects.toMatchObject({ code: "ENOENT" });
  });
});

it("the build CLI accepts a bundled data URL without treating it as a file", async () => {
  await project(async (directory) => {
    await writeFile(
      join(directory, "operator.ts"),
      'import value from "data:text/javascript,export default 1"; export const handler = () => value;',
    );
    const result = cli(directory, "operator.ts");
    expect(result.status).toBe(0);
    expect(result.output).toContain("node build.mjs -- operator.ts");
    const metadata = JSON.parse(await readFile(join(directory, "dist/worker-meta.json"), "utf8"));
    expect(Object.keys(metadata.inputs).some((path) => path.includes("data:text/javascript"))).toBe(
      true,
    );
    const exported = createRequire(import.meta.url)(join(directory, "dist/worker.cjs"));
    expect(exported.handler()).toBe(1);
  });
});

it("normal builds do not execute operator code or certify handler behavior", async () => {
  await project(async (directory) => {
    await writeFile(
      join(directory, "operator.ts"),
      'throw Error("must not execute"); export const notAHandler = 1;',
    );
    expect(cli(directory, "operator.ts")).toMatchObject({ status: 0 });
    expect((await readFile(join(directory, "dist/worker.cjs"))).length).toBeGreaterThan(0);
  });
});

for (const kind of ["entry", "metadata", "symlink", "transitive"] as const) {
  it(`build rejects ${kind} input/output collision without changing authored input`, async () => {
    await project(async (directory) => {
      await mkdir(join(directory, "dist"));
      const path =
        kind === "metadata"
          ? "dist/worker-meta.json"
          : kind === "transitive"
            ? "dist/api.cjs"
            : "dist/worker.cjs";
      const source = kind === "metadata" ? '{"value":1}' : 'exports.handler = () => "authored";';
      await writeFile(join(directory, path), source);
      let entry = path;
      if (kind === "symlink") {
        entry = "operator.cjs";
        await symlink(path, join(directory, entry));
      } else if (kind === "transitive") {
        entry = "operator.ts";
        await writeFile(join(directory, entry), 'export { handler } from "./dist/api.cjs";');
      }
      const result = cli(directory, entry);
      expect(result.status).not.toBe(0);
      expect(result.output).toContain("Build input collides with an output");
      expect(await readFile(join(directory, path), "utf8")).toBe(source);
      expect(await readdir(join(directory, "dist"))).toEqual([path.slice(5)]);
    });
  });
}

it("build output replacement preserves an authored hard-link alias", async () => {
  await project(async (directory) => {
    const source = 'exports.handler = () => "authored";';
    await mkdir(join(directory, "dist"));
    await writeFile(join(directory, "operator.cjs"), source);
    await link(join(directory, "operator.cjs"), join(directory, "dist/worker.cjs"));
    expect(cli(directory, "operator.cjs")).toMatchObject({ status: 0 });
    expect(await readFile(join(directory, "operator.cjs"), "utf8")).toBe(source);
    expect(await readFile(join(directory, "dist/worker.cjs"), "utf8")).not.toBe(source);
  });
});

it("default build refuses a symlinked worker output rather than deleting its target", async () => {
  await project(async (directory) => {
    await mkdir(join(directory, "dist"));
    await writeFile(join(directory, "authored.cjs"), "authored");
    await symlink("../authored.cjs", join(directory, "dist/worker.cjs"));
    expect(cli(directory).status).not.toBe(0);
    expect(await readFile(join(directory, "authored.cjs"), "utf8")).toBe("authored");
    expect((await lstat(join(directory, "dist/worker.cjs"))).isSymbolicLink()).toBe(true);
  });
});
