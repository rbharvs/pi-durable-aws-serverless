import { EventEmitter } from "node:events";
import pg from "pg";
import { afterEach, it, expect, vi } from "vitest";
import { connect, isPostgresAbort, postgresProvider, schemaIdentifier } from "./connection.ts";

afterEach(() => vi.restoreAllMocks());

function offlineClients(fail?: "connect" | "query") {
  const configs: pg.ClientConfig[] = [];
  const clients: OfflineClient[] = [];

  class OfflineClient extends EventEmitter {
    connect = vi.fn(async () => {
      if (fail === "connect") throw new Error("synthetic connect failure");
    });
    query = vi.fn(async (_sql: string) => {
      if (fail === "query") throw new Error("synthetic schema setter failure");

      return { rows: [] };
    });
    end = vi.fn(async () => {
      if (fail) throw new Error("synthetic close failure");
    });

    constructor(config: pg.ClientConfig) {
      super();
      configs.push(config);
      clients.push(this);
    }
  }

  const constructor = vi.spyOn(pg, "Client").mockImplementation(function (config: pg.ClientConfig) {
    return new OfflineClient(config);
  } as unknown as typeof pg.Client);

  return { configs, clients, constructor };
}

it("accepts 63 ASCII schema characters but rejects both 64-character equal-prefix names before connection", async () => {
  const { constructor } = offlineClients();
  const prefix = "s".repeat(63);

  expect(schemaIdentifier(prefix)).toBe(`"${prefix}"`);

  for (const name of [prefix + "a", prefix + "b", "Upper", "é", "1schema", 'a"b']) {
    expect(() => schemaIdentifier(name)).toThrow("Invalid schema identifier");
    expect(() => postgresProvider(5432, "synthetic-password", name)).toThrow(
      "Invalid schema identifier",
    );
    await expect(connect({}, name)).rejects.toThrow("Invalid schema identifier");
  }

  expect(constructor).not.toHaveBeenCalled();
});

it("the concrete pg boundary keeps finite timeouts, one schema setter and idle-error handling", async () => {
  const { configs, clients } = offlineClients();
  const log = vi.spyOn(console, "error").mockImplementation(() => {});
  const client = await postgresProvider(5432, "synthetic-password", "agent_service").connect();

  try {
    expect(configs).toHaveLength(1);
    expect(configs[0]).toMatchObject({
      host: "127.0.0.1",
      port: 5432,
      user: "postgres",
      database: "postgres",
      connectionTimeoutMillis: 15000,
      query_timeout: 25000,
    });
    expect(clients[0].connect).toHaveBeenCalledTimes(1);
    expect(clients[0].query.mock.calls).toEqual([['SET search_path TO "agent_service"']]);
    expect(clients[0].listenerCount("error")).toBe(1);
    expect(() => clients[0].emit("error", new Error("synthetic idle socket fault"))).not.toThrow();
    expect(log).toHaveBeenCalledWith("pg-socket", "Error");
    expect(JSON.stringify(log.mock.calls)).not.toContain("synthetic idle socket fault");
  } finally {
    await client.end();
  }

  expect(clients[0].end).toHaveBeenCalledTimes(1);
});

for (const fail of ["connect", "query"] as const) {
  it(`a failed ${fail} during acquisition closes the client and propagates its own cause`, async () => {
    const { clients } = offlineClients(fail);

    await expect(connect({}, "agent_service")).rejects.toThrow(
      fail === "connect" ? "synthetic connect failure" : "synthetic schema setter failure",
    );
    expect(clients).toHaveLength(1);
    expect(clients[0].end).toHaveBeenCalledTimes(1);
  });
}

it("rejects invalid local ports before constructing a client", () => {
  const { constructor } = offlineClients();

  for (const port of [0, 65536, 1.5, Number.NaN]) {
    expect(() => postgresProvider(port, "synthetic-password", "public")).toThrow(
      "Invalid local port",
    );
  }
  expect(constructor).not.toHaveBeenCalled();
});

it("retries only provider-confirmed serialization and deadlock aborts", () => {
  const coded = (code: unknown) => Object.assign(new Error("synthetic provider fault"), { code });

  for (const code of ["40001", "40P01"]) {
    expect(isPostgresAbort(coded(code))).toBe(true);
  }
  // Termination, connection loss and constraint failures have unknown or non-retryable outcomes.
  for (const error of [
    coded("57P01"),
    coded("08006"),
    coded("ECONNRESET"),
    coded("23505"),
    coded(40001),
    coded(null),
    coded(undefined),
    new Error("40001"),
    { message: "40001" },
    "40001",
    null,
    undefined,
  ]) {
    expect(isPostgresAbort(error)).toBe(false);
  }
});
