import { mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { serializeProperties } from "@pulumi/pulumi/runtime/rpc.js";
import { expect, it } from "vitest";
import { artifact } from "../src/artifacts.ts";

it("prebuilt archives serialize distinct absolute file references, not bundle text, and reject symlinks", async () => {
  const directory = mkdtempSync(join(tmpdir(), "durable-artifacts-"));
  const url = pathToFileURL(directory + "/");
  try {
    for (const name of ["api", "worker", "reconciler"] as const) {
      const bytes = `exports.handler = () => "${name}-prebuilt";`;
      const path = join(directory, `${name}.cjs`);
      writeFileSync(path, bytes);
      const result = await serializeProperties("code", { code: artifact(name, url) });
      const files = result.code.assets;
      expect(Object.keys(files)).toEqual([`${name}.cjs`]);
      expect(files[`${name}.cjs`].path).toBe(path);
      expect(files[`${name}.cjs`]).not.toHaveProperty("text");
      expect(JSON.stringify(result)).not.toContain(bytes);
      expect(readFileSync(files[`${name}.cjs`].path, "utf8")).toBe(bytes);
    }
    rmSync(join(directory, "worker.cjs"));
    expect(() => artifact("worker", url)).toThrow(
      "Missing regular prebuilt artifact: worker.cjs; run pnpm build -- <worker-entry>",
    );
    symlinkSync(join(directory, "api.cjs"), join(directory, "worker.cjs"));
    expect(() => artifact("worker", url)).toThrow("regular prebuilt artifact");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
