import { DsqlSigner } from "@aws-sdk/dsql-signer";
import { check, errorCode } from "../domain.ts";
import { connect, schemaIdentifier, type SqlProvider } from "../sql/connection.ts";
import { region, type AwsOptions } from "./config.ts";

/** Only the SQLSTATE confirms an aborted transaction; an OC000 message is not enough. */
export const isDsqlAbort = (error: unknown): boolean => errorCode(error) === "40001";

export function dsqlSigner(host: string, options: AwsOptions): DsqlSigner {
  const configuredRegion = region(options.region);
  check(
    host.length <= 253 &&
      new RegExp(`^[a-z0-9]{1,63}\\.dsql\\.${configuredRegion}\\.on\\.aws$`).test(host),
    "Unexpected DSQL host",
  );
  return new DsqlSigner({
    hostname: host,
    region: configuredRegion,
    credentials: options.credentials,
    expiresIn: 900,
  });
}

export function dsqlProvider(host: string, schema: string, options: AwsOptions): SqlProvider {
  schemaIdentifier(schema);
  const signer = dsqlSigner(host, options);
  return {
    // Sign for each connection; no token is retained by this provider or logged.
    connect: async () =>
      connect(
        {
          host,
          port: 5432,
          user: "admin",
          database: "postgres",
          password: await signer.getDbConnectAdminAuthToken(),
          ssl: { rejectUnauthorized: true },
        },
        schema,
      ),
    isRetryableAbort: isDsqlAbort,
  };
}
