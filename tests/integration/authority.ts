/** Fixture-only SQL inspection, excluded from the public Authority surface. */
import { SqlAuthority } from "../../src/sql/authority.ts";
import type { Database } from "../../src/sql/db.ts";
import type { SqlProvider } from "../../src/sql/connection.ts";

/** An `SqlAuthority` whose database handle tests may query directly. */
export class InspectableAuthority extends SqlAuthority {
  constructor(
    readonly inspectionDb: Database,
    provider: SqlProvider,
  ) {
    super(inspectionDb, provider);
  }
}
