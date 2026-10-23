import { modelRuntimeTests } from "../shared/model-runtime.ts";
import { namespace, provider } from "./postgres.ts";
import { createSqlBackend } from "../../src/sql/backend.ts";
import { DB_NOW_MS } from "../../src/sql/authority.ts";
import { objectFixture } from "../shared/objects.ts";

modelRuntimeTests(async () => {
  const f = await namespace("model_runtime");
  const objects = objectFixture();
  return {
    backend: createSqlBackend(provider, objects.objects),
    ns: f.ns,
    expire: () =>
      f.authority.inspectionDb.query(`UPDATE harness SET lease_until=${DB_NOW_MS}-1 WHERE ns=$1`, [
        f.ns,
      ]),
    close: f.finish,
  };
});
