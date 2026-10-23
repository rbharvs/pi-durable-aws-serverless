import { lstatSync } from "node:fs";
import { fileURLToPath } from "node:url";
import * as pulumi from "@pulumi/pulumi";
import type { FunctionName } from "./stack.js";

export function artifact(
  name: FunctionName,
  directory = new URL("../../dist/", import.meta.url),
): pulumi.asset.AssetArchive {
  const file = new URL(`${name}.cjs`, directory);
  if (!lstatSync(file, { throwIfNoEntry: false })?.isFile()) {
    throw new Error(
      `Missing regular prebuilt artifact: ${name}.cjs; run pnpm build -- <worker-entry>`,
    );
  }
  // Absolute file paths remain correct regardless of the language host's working directory.
  return new pulumi.asset.AssetArchive({
    [`${name}.cjs`]: new pulumi.asset.FileAsset(fileURLToPath(file)),
  });
}
