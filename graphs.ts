import { build, type BuildResult, type Metafile } from "esbuild";

export const roots = {
  worker: "src/aws/worker.ts", // Factory only; a deployable worker needs an explicit entry.
  api: "src/aws/api.ts",
  reconciler: "src/aws/reconciler.ts",
  index: "src/index.ts",
} as const;
export type Root = keyof typeof roots;
export type Graph = BuildResult & { metafile: Metafile };

/** One configuration for checked artifacts and packaging tests. Nothing executes the entry. */
export function bundleGraph(
  root: Root,
  entry: string = roots[root],
  directory = process.cwd(),
): Promise<Graph> {
  return build({
    absWorkingDir: directory,
    entryPoints: [entry],
    outfile: `dist/${root}.cjs`,
    bundle: true,
    platform: "node",
    target: "node24",
    format: "cjs",
    write: false,
    metafile: true,
    tsconfig: "tsconfig.json",
  });
}

export async function bundleGraphs(
  workerEntry: string = roots.worker,
): Promise<Record<Root, Graph>> {
  return Object.fromEntries(
    await Promise.all(
      (Object.keys(roots) as Root[]).map(async (root) => [
        root,
        await bundleGraph(root, root === "worker" ? workerEntry : roots[root]),
      ]),
    ),
  ) as Record<Root, Graph>;
}

const categories = {
  execution: (p: string) =>
    /^src\/(runtime|memory)\.ts$/.test(p) ||
    /^src\/sql\/(storage|backend)\.ts$/.test(p) ||
    /@earendil-works\/pi-durable\/dist\/(harness|storage)\//.test(p),
  s3: (p: string) => p === "src/aws/s3.ts" || p.includes("@aws-sdk/client-s3/"),
  dispatcher: (p: string) => p === "src/aws/dispatch.ts",
  sql: (p: string) => p.startsWith("src/sql/") || p.includes("node_modules/pg/"),
  aws: (p: string) => p.startsWith("src/aws/") || p.includes("@aws-sdk/"),
};
const boundaries: Record<Root, (keyof typeof categories)[]> = {
  worker: ["dispatcher"],
  api: ["execution"],
  reconciler: ["execution", "s3"],
  index: ["sql", "aws"],
};

/** Packaging regression checks, not authority or behavior proofs. Removed imports are allowed. */
export function assertGraphBoundaries(root: Root, graph: Graph): void {
  const contributing = Object.values(graph.metafile.outputs).flatMap((output) =>
    Object.entries(output.inputs)
      .filter(([, input]) => input.bytesInOutput > 0)
      .map(([path]) => path),
  );
  for (const category of boundaries[root]) {
    const forbidden = contributing.filter(categories[category]);
    if (forbidden.length)
      throw Error(`Forbidden ${root} dependencies (${category}): ${forbidden.join(", ")}`);
  }
}
