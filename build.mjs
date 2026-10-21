import { lstatSync, mkdirSync, realpathSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { bundleGraphs, assertGraphBoundaries } from "./graphs.ts";

// pnpm forwards the optional separator as well as the entry.
const args = process.argv.slice(2);
if (args[0] === "--") args.shift();
if (args.length > 1) throw new Error("Usage: pnpm build -- [worker-entry]");
const graphs = await bundleGraphs(args[0]);
for (const [root, graph] of Object.entries(graphs)) {
  assertGraphBoundaries(root, graph);
}

mkdirSync("dist", { recursive: true });
if (lstatSync("dist").isSymbolicLink()) throw new Error("dist must not be a symlink");
const directory = realpathSync("dist");
// These six paths are build-owned. Check all inputs before writing or clearing stale outputs.
const names = ["api", "reconciler", "worker"];
const outputs = names
  .flatMap((name) => [`${name}.cjs`, `${name}-meta.json`])
  .map((name) => resolve(directory, name));
// esbuild includes entries and resolves file symlinks; some inputs (e.g. data URLs) are not files.
const inputs = Object.values(graphs).flatMap((graph) => Object.keys(graph.metafile.inputs));
for (const input of inputs) {
  if (outputs.includes(resolve(input))) {
    throw new Error(`Build input collides with an output: ${input}`);
  }
}
for (const output of outputs) {
  const stat = lstatSync(output, { throwIfNoEntry: false });
  if (stat && !stat.isFile()) throw new Error(`Build output must be a regular file: ${output}`);
}

// Replace rather than truncate: an existing output's hard-link aliases must stay untouched.
function write(path, bytes) {
  const temporary = `${path}.${randomUUID()}.tmp`;
  try {
    writeFileSync(temporary, bytes, { flag: "wx" });
    renameSync(temporary, path);
  } finally {
    rmSync(temporary, { force: true });
  }
}

if (!args.length) {
  rmSync("dist/worker.cjs", { force: true });
  rmSync("dist/worker-meta.json", { force: true });
}
for (const name of args.length ? names : ["api", "reconciler"]) {
  const graph = graphs[name];
  write(`dist/${name}.cjs`, graph.outputFiles[0].contents);
  write(`dist/${name}-meta.json`, JSON.stringify(graph.metafile, null, 2));
}
