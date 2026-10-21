import {
  contracts,
  input,
  publishSubmission,
  type ContractFixture,
} from "../shared/storage.contract.ts";
import { namespace, provider } from "./postgres.ts";
import { createSqlBackend } from "../../src/sql/backend.ts";
import { objectFixture } from "../shared/objects.ts";
import { check, harnessId, integer, requestId, sleep } from "../../src/domain.ts";
import { idFromNumber } from "../../src/ids.ts";

async function fixture(): Promise<ContractFixture> {
  const f = await namespace("parity");
  try {
    const backend = createSqlBackend(provider, objectFixture().objects);
    const seed = (await f.authority.receive(f.ns, requestId("seed"), input)).item;
    const c = await f.authority.claim(f.ns, 45000);
    check(c.kind === "claimed" && seed.kind === "received", "fixture claim");
    await publishSubmission({ backend, lease: c.lease }, 2);
    await f.authority.markAdmitted(seed, idFromNumber(2));
    return {
      ...f,
      backend,
      lease: c.lease,
      firstId: 3,
      expire: async () => {
        await f.authority.inspectionDb.query("UPDATE harness SET lease_until=0 WHERE ns=$1", [
          f.ns,
        ]);
      },
      advanceAuthorityClock: sleep,
      setExpiredLeases: async (expiries) => {
        const actual = [];
        for (const { ns, until } of expiries) {
          const rows = await f.authority.inspectionDb.query(
            "UPDATE harness SET lease_until=$2 WHERE ns=$1 RETURNING ns,lease_until",
            [ns, until],
          );
          check(rows.length === 1, "Missing fixture expiry namespace");
          actual.push({ ns: harnessId(rows[0]!.ns), until: integer(rows[0]!.lease_until) });
        }
        return actual;
      },
      orderingTies: async () => {
        await f.authority.inspectionDb.query(
          "UPDATE inbox SET received_at=1 WHERE starts_with(ns,$1)",
          [f.ns],
        );
        await f.authority.inspectionDb.query(
          "UPDATE harness SET lease_until=0 WHERE starts_with(ns,$1) AND ns<>$1",
          [f.ns],
        );
      },
    };
  } catch (error) {
    await f.finish();
    throw error;
  }
}

import { boundaries } from "../shared/backend-boundaries.ts";
contracts("PostgreSQL/shared Storage", fixture);
boundaries("PostgreSQL", fixture);
