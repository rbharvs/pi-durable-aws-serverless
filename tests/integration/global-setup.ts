import { execFile, execFileSync, spawnSync } from "node:child_process";
import { randomBytes, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { setTimeout as delay } from "node:timers/promises";
import type { TestProject } from "vitest/node";
import { postgresProvider } from "../../src/sql/connection.ts";

const image =
  "postgres:16.13@sha256:5d143123fdf80462d1778cd4f24b9f7ca13c87174bca19141fb194c5a1ebca59";

const projectLabel = "pi-durable-aws-serverless-tests";

function docker(args: string[], timeout = 30000): Promise<string> {
  return new Promise((resolve, reject) =>
    execFile("docker", args, { timeout, maxBuffer: 1024 * 1024 }, (error, stdout) => {
      if (error) reject(Error("Owned PostgreSQL Docker command failed"));
      else resolve(stdout.trim());
    }),
  );
}

function recoveredId(name: string): string | undefined {
  const result = spawnSync(
    "docker",
    ["inspect", "--format", '{{.Id}} {{index .Config.Labels "project"}}', name],
    { encoding: "utf8", timeout: 10000 },
  );
  const found = result.stdout?.trim();

  return result.status === 0 && new RegExp(`^[a-f0-9]{64} ${projectLabel}$`).test(found)
    ? found.split(" ")[0]
    : undefined;
}

export default async function setup(project: TestProject) {
  const name = `durable-test-${randomUUID()}`,
    password = randomBytes(32).toString("base64url");
  let id: string | undefined,
    cleaned = false;
  const cleanup = () => {
    if (!id || cleaned) return;

    const removed = spawnSync("docker", ["rm", "--force", id], {
      encoding: "utf8",
      timeout: 15000,
    });
    const absent = spawnSync("docker", ["inspect", "--format", "{{.Id}}", id], {
      encoding: "utf8",
      timeout: 10000,
    });

    if (
      removed.status !== 0 ||
      absent.status !== 1 ||
      !/no such (object|container)/i.test(absent.stderr ?? "")
    )
      throw Error(`Owned PostgreSQL cleanup failed for ${id}`);
    cleaned = true;
    console.log(`Owned PostgreSQL removed and verified absent: ${id}`);
  };
  const interrupt = (code: number) => {
    try {
      cleanup();
    } finally {
      process.exit(code);
    }
  };
  const onInt = () => interrupt(130),
    onTerm = () => interrupt(143),
    onExit = () => cleanup();
  const detach = () => {
    process.off("SIGINT", onInt);
    process.off("SIGTERM", onTerm);
    process.off("exit", onExit);
  };

  // Register BEFORE synchronous acquisition: default Node signal handlers can exit during execFileSync.
  // Queued delivery now sees the acquired ID, or its exact unique-name+label recovery.
  process.on("SIGINT", onInt);
  process.on("SIGTERM", onTerm);
  process.once("exit", onExit);

  try {
    try {
      const created = execFileSync(
        "docker",
        [
          "create",
          "--name",
          name,
          "--label",
          `project=${projectLabel}`,
          "--cpus",
          "1",
          "--memory",
          "1536m",
          "--pids-limit",
          "128",
          "--tmpfs",
          "/var/lib/postgresql/data:rw,size=1073741824",
          "-p",
          "127.0.0.1::5432",
          "-e",
          "POSTGRES_PASSWORD",
          "-e",
          "POSTGRES_INITDB_ARGS=--encoding=UTF8 --locale=C",
          "-e",
          "TZ=UTC",
          image,
        ],
        {
          env: { ...process.env, POSTGRES_PASSWORD: password },
          encoding: "utf8",
          timeout: 30000,
          stdio: "pipe",
        },
      ).trim();

      if (!/^[a-f0-9]{64}$/.test(created)) throw Error("Invalid owned PostgreSQL ID");
      id = created;
    } catch {
      id = recoveredId(name);

      throw Error("Owned PostgreSQL creation failed");
    }

    console.log(`Owned PostgreSQL created: ${id}`);
    await docker(["start", id]);

    const deadline = Date.now() + 60000;

    for (;;) {
      if (Date.now() >= deadline) throw Error("Owned PostgreSQL readiness deadline");

      try {
        await docker(
          ["exec", id, "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-t", "1"],
          Math.min(5000, deadline - Date.now()),
        );
        break;
      } catch {
        await delay(200);
      }
    }

    const binding = await docker(["port", id, "5432/tcp"]);

    if (!/^127\.0\.0\.1:\d+$/.test(binding)) throw Error("Invalid local PostgreSQL binding");

    const port = Number(binding.split(":")[1]),
      base = postgresProvider(port, password, "public");

    const client = await base.connect();

    try {
      await client.query('CREATE SCHEMA "integration"');
    } finally {
      await client.end();
    }

    const scoped = await postgresProvider(port, password, "integration").connect();

    try {
      await scoped.query(
        readFileSync(new URL("../../src/sql/schema.sql", import.meta.url), "utf8"),
      );
    } finally {
      await scoped.end();
    }

    project.provide("postgres", { port, password });

    return () => {
      try {
        cleanup();
      } finally {
        detach();
      }
    };
  } catch {
    try {
      cleanup();
    } finally {
      detach();
    }

    throw Error("Owned local PostgreSQL setup failed; secret configuration withheld");
  }
}
