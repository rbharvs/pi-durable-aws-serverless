/** Authority-only configuration: no Storage, Harness, S3, or invocation capability. */
import { region, required, type AwsOptions } from "./config.ts";
import { dsqlProvider } from "./dsql.ts";
import { SqlAuthority } from "../sql/authority.ts";
import { Database } from "../sql/db.ts";
import type { Authority } from "../authority.ts";

export interface LambdaContext {
  awsRequestId: string;
  getRemainingTimeInMillis(): number;
}

/** Validates environment configuration now; each connectAuthority call acquires its own SQL handle. */
export function configuration(aws: AwsOptions = { region: region(process.env.AWS_REGION) }) {
  const provider = dsqlProvider(
    required(process.env.DSQL_HOST, "DSQL host"),
    required(process.env.DSQL_SCHEMA, "database schema"),
    aws,
  );
  return {
    aws,
    provider,
    connectAuthority: async (): Promise<Authority> =>
      new SqlAuthority(await Database.connect(provider), provider),
  };
}

/** Builds a synchronous JSON sink with invocation identity; writing or serialization may throw. */
export function logger(source: "api" | "worker" | "reconciler", ctx: LambdaContext) {
  return (event: string, fields: Record<string, unknown>) => {
    console.log(
      JSON.stringify({
        ...fields,
        event,
        at: Date.now(),
        source,
        invocation: ctx.awsRequestId,
      }),
    );
  };
}
