import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";

const { scripts } = JSON.parse(
  readFileSync(new URL("../../package.json", import.meta.url), "utf8"),
);
const entry = "/owned application/worker $(not-a-command).ts";
const steps = [
  ["pnpm", "check"],
  ["pnpm", "test"],
  ["pnpm", "test:integration"],
  ["pnpm", "build", "--", entry],
  ["pnpm", "infra:build"],
  ["pulumi", "-C", "infra", "up"],
];

function run(workerEntry: string | undefined, failAt = "") {
  expect(typeof scripts.deploy).toBe("string");
  const directory = mkdtempSync(join(tmpdir(), "durable-deploy-command-"));
  try {
    const log = join(directory, "calls.jsonl");
    const state = join(directory, "state");
    writeFileSync(log, "");
    writeFileSync(state, "unchanged");
    // Only these owned stubs are reachable through PATH. No cloud/config access.
    for (const name of ["pnpm", "pulumi"]) {
      writeFileSync(
        join(directory, name),
        `#!${process.execPath}
const fs = require("node:fs");
const args = process.argv.slice(2);
fs.appendFileSync(process.env.LOG, JSON.stringify([${JSON.stringify(name)}, ...args]) + "\\n");
if (${JSON.stringify(name)} + ":" + args[0] === process.env.FAIL_AT) process.exit(42);
if (${JSON.stringify(name)} === "pulumi") {
  process.stdout.write(fs.readFileSync(0));
  fs.writeFileSync(process.env.STATE, "applied");
}
`,
        { mode: 0o700 },
      );
    }
    const result = spawnSync("/bin/sh", ["-c", scripts.deploy], {
      cwd: directory,
      env: {
        PATH: directory,
        HOME: directory,
        LOG: log,
        STATE: state,
        FAIL_AT: failAt,
        ...(workerEntry === undefined ? {} : { WORKER_ENTRY: workerEntry }),
      },
      input: "operator confirmation\n",
      encoding: "utf8",
      timeout: 10000,
    });
    if (result.error) throw result.error;
    return {
      ...result,
      calls: readFileSync(log, "utf8")
        .trim()
        .split("\n")
        .filter(Boolean)
        .map((line) => JSON.parse(line)),
      state: readFileSync(state, "utf8"),
    };
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

it("deploy checks, tests and builds before interactive Pulumi, preserving the entry argument", () => {
  const result = run(entry);
  expect(result.status).toBe(0);
  expect(result.calls).toEqual(steps);
  expect(result.stdout).toBe("operator confirmation\n");
  expect(result.state).toBe("applied");
});

for (const [index, step] of steps.entries()) {
  it(`deploy stops after failure of ${step.join(" ")}`, () => {
    const result = run(entry, `${step[0]}:${step[1]}`);
    expect(result.status).toBe(42);
    expect(result.calls).toEqual(steps.slice(0, index + 1));
    expect(result.state).toBe("unchanged");
  });
}

for (const missing of [undefined, ""]) {
  it(`deploy uses the production entry with no override (${String(missing)})`, () => {
    const result = run(missing);
    expect(result.status).toBe(0);
    expect(result.calls).toEqual(
      steps.map((step, index) =>
        index === 3 ? ["pnpm", "build", "--", "src/aws/worker-entry.ts"] : step,
      ),
    );
    expect(result.stdout).toBe("operator confirmation\n");
    expect(result.state).toBe("applied");
  });
}
