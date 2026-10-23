# Local deployment

Ordinary Pulumi TypeScript: build the application, then run interactive `pulumi up`.
No builds or SQL run inside Pulumi. CI validates only; it never deploys.

The stack creates regional DSQL, a private versioned SSE-S3 payload bucket, three
Node 24/arm64 Lambdas with separate execution roles, logs, and a one-minute Scheduler.
Invocation is direct IAM-authenticated Lambda Invoke. There is no HTTP endpoint,
VPC, NAT, CloudFront, domain or customer-managed KMS key.

## Setup (separate approval before any AWS access)

Use an existing-account, non-root deployment-role profile in `us-east-1`. Separately
bootstrap a **private, versioned, SSE-S3 state bucket** with all public access blocked,
BucketOwnerEnforced ownership and a TLS-only bucket policy. Keep state versions;
privately back up state, the passphrase and gitignored `infra/Pulumi.<stack>.yaml`.
The YAML contains encrypted secrets **and plaintext private configuration/IDs**; keep
it outside Git. Pulumi does not manage its own backend. State backups do not replace
data backups.

`bootstrap-trust.json.example` and `bootstrap-policy.json.example` are reviewable,
scoped IAM starting points, not automated bootstrap or live-validated policies.
Replace placeholders in a private copy outside Git. The state prefix below is
`pi-durable`; resource prefix defaults to `pi-durable`. Tighten DSQL's cluster wildcard
after creation. DSQL's service-linked role may need separate account bootstrap.
Runtime roles cannot manage infrastructure or delete payloads. Client invocation,
staging uploads and destructive cleanup permissions are separate operator grants.

From the repository root, enter the existing pinned environment:

```sh
nix develop --no-update-lock-file
pnpm install --frozen-lockfile
export AWS_PROFILE='your-deployment-role-profile'
export AWS_REGION=us-east-1
export AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true
aws --profile "$AWS_PROFILE" --region "$AWS_REGION" sts get-caller-identity
```

Verify the expected account and assumed role, never root. Remove unrelated ambient
credentials/endpoint overrides. Profile selection and confirmations use standard
AWS/Pulumi behavior, not a custom identity gate.

After the separately approved backend bootstrap, set your private bucket and account:

```sh
export STATE_BUCKET='your-private-state-bucket'
export ACCOUNT_ID='your-12-digit-account-id'
export PULUMI_BACKEND_URL="s3://$STATE_BUCKET/pi-durable?region=us-east-1&awssdk=v2"
pulumi -C infra stack init personal --secrets-provider passphrase
pulumi -C infra config set aws:profile "$AWS_PROFILE"
pulumi -C infra config set aws:region us-east-1
pulumi -C infra config set accountId "$ACCOUNT_ID"
```

The provider restricts resource operations to `accountId`; verify the caller **before**
backend access too. Backend selection above is shell-local: re-export
`PULUMI_BACKEND_URL` with the same private bucket URL in each new terminal before
any Pulumi command, and restore the deployment profile/region. Do not rerun stack
initialization for an existing stack. Let Pulumi prompt for the passphrase, or inject
`PULUMI_CONFIG_PASSPHRASE` using your password manager. Stack YAML is gitignored.
Keep personal IDs, credentials, passphrases, state and application secrets out of Git.

Optional config: `prefix` (3–27 lowercase name characters), `schema` (default
`durable`), and `allowDataDeletion` (default **false**).
Choose `prefix` and `schema` before the first apply; later changes require a manual
migration, not just a config edit.
Store provider API keys in the passphrase-encrypted stack config:

```sh
pulumi -C infra config set --secret workerEnvironment
```

At the hidden prompt, enter a top-level JSON object with **string** values named
`OPENAI_API_KEY` and/or `ANTHROPIC_API_KEY`, matching the providers your entry uses.
These are the pinned public Pi providers' environment key names. Never put real
values in command arguments/history, source files or bundles. The stack passes
this secret object only to the worker Lambda environment, not API/reconciler.
It does not create Secrets Manager resources or a customer-managed KMS key;
Lambda uses its default encryption at rest. Authorized Lambda/config readers can
still access the keys. Provider keys authenticate requests; they do **not** select
a model.
Optional `workerPolicyStatements` is an IAM statement array for the existing worker's
actual tools/provider. Grant only needed actions/resources. Do not inject AWS
credentials, endpoint overrides or TLS-disabling settings. Infrastructure-owned
DSQL/bucket values override application environment values.

## Build and apply

The supplied [production entry](../src/aws/worker-entry.ts) exports the Lambda
handler and registers Pi's official `openaiProvider()` and `anthropicProvider()`
with `createModels()` and `createRegistry()`. It uses the provider implementations
shipped by pinned Pi 1.3.0, not a fake provider or a new execution engine.

No model is selected at deployment. Callers supply `model: { provider, modelId }`
in input; Pi persists the choice. See the [input contract and examples](../README.md#usage)
for continuation, changes while busy, validation and durable failure outcomes.
Configure the corresponding API keys using `workerEnvironment` above. No model
credentials file is loaded; auth resolution is limited to those two environment keys.

No worker assembly or entry-path setup is needed. Custom applications may optionally
set `WORKER_ENTRY` in their private deployment shell environment to a separately tested
entry exporting `handler = createAwsHandler(harnessOptions, rootAgent)`. Unset or
empty uses the supplied production entry, never a fixture or old bundle. The build
checks dependency boundaries; it does not execute/certify arbitrary custom handlers.

Infra keeps TypeScript 6 because the pinned Pulumi SDK requires a `<7` peer; the
application stays on TypeScript 7.

After setup, run one command from the root, inside the Nix shell:

```sh
pnpm run deploy
```

Use **`pnpm run deploy`**, not `pnpm deploy` (a different built-in pnpm command).
It runs checks, offline and infrastructure tests, PostgreSQL integration tests
(requires Docker), builds all three Lambdas with the production entry (or your
optional `WORKER_ENTRY` override), builds infra, then
runs interactive `pulumi -C infra up`. Each step must succeed before the next runs.
No `--yes` or schema initialization is included.

Review Pulumi's selected stack and diff, then use its normal confirmation. Do not
pass `--yes` or mutate/rebuild artifacts between preview and apply. Each archive
contains its own prebuilt `.cjs` file using FileAsset, not embedded checkpoint text.
`pnpm build` without an entry deliberately removes stale worker output and cannot
produce a deployable worker; `pnpm run deploy` always passes the concrete entry.

Every Lambda uses ordinary unreserved concurrency from the account pool. The pinned
AWS provider documents `reservedConcurrentExecutions: -1` as removing function
concurrency limits, including an existing zero reservation. Account concurrency
quotas still apply; this stack creates no dedicated reservations or quota increases.
The one-minute schedule is enabled. Record the output names/ARNs for operations.

## Initialize DSQL once before API requests

After the first apply, initialize a fresh schema explicitly. This is an operator
step, not a Pulumi dependency or migration protocol. Do not submit API requests until
initialization and inspection finish. Scheduled reconciliation may briefly fail
before the first schema is ready; this is a first-bootstrap window, not a normal
pause phase. Scheduler and Lambda async retries are disabled, with a 60-second
maximum event age; the next scheduled minute tries again. Initialize promptly.
An already initialized schema does not need this step again.
Review `src/sql/schema.sql` before running:

```sh
export PGHOST="$(pulumi -C infra stack output dsqlHost)"
export PGPORT=5432 PGUSER=admin PGDATABASE=postgres
PGPASSWORD="$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" \
  dsql generate-db-connect-admin-auth-token --hostname "$PGHOST")" \
  node infra/dist/init-schema.js "$(pulumi -C infra stack output schema)"
```

The small script uses verified TLS, creates a **new** schema, executes the existing
reference statements with `CREATE INDEX` changed to `CREATE INDEX ASYNC`, and waits
for each returned job to succeed. It preserves CHECK constraints and the partial
index. The current reference SQL uses simple semicolon-delimited statements; review
the script if that format changes. There are no hashes, receipts or adoption logic.

A successful run creates six tables and nine secondary indexes. Confirm the tables
against `src/sql/schema.sql`; the query below must list nine secondary indexes, all
valid, before proceeding (substitute your schema if not `durable`):

```sql
SELECT c.relname, i.indisvalid
FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'durable' AND NOT i.indisprimary
ORDER BY c.relname;
```

If initialization fails, **do not submit API requests**. Some DDL may already have succeeded.
If recovery will take time, use the manual stop below to avoid recurring scheduler errors.
Inspect the completed-statement output, catalog and jobs; a failed statement may
also have taken effect. Re-running on an existing schema fails. For an unused first
bootstrap only, **verify no application use or data and obtain explicit approval for
any drop** before manually removing partial objects/schema and restarting. Never
assume it is empty. Alternatively, finish remaining DDL manually: first set
`SET search_path TO durable;` (substitute your chosen schema) in that SQL session,
then use `CREATE INDEX ASYNC` and wait for each job. An invalid index may remain
after a failed job; inspect it and obtain approval before dropping/recreating it.
Do not treat an existing index name as success: confirm all nine are valid before
submitting API requests. No automatic drop or retry occurs.
Later schema changes require reviewed manual migrations.
Verify DSQL catalogs and job results for each fresh bootstrap; local PostgreSQL
alone is not proof of DSQL compatibility. Idle/inactive DSQL clusters wake on
connection; a bounded connection timeout can require a later retry.

After initialization and inspection, submit API requests; no second deployment is
needed to enable compute.

**The operator owns this precondition.** Pulumi does not inspect the live schema or
certify readiness. Invoke only the API ARN with the existing `{ ns, request, command }`
payload contract; worker/reconciler are internal. Monitor CloudWatch errors and
throttles, Scheduler failures, DSQL usage and S3 costs. No runtime object/history
cleanup is provided.

## Manual stop, recovery and teardown

There is no routine pause setting. Recover by redeploying known-good artifacts after
reviewing schema compatibility. A normal update can change code/IAM/config and restore
unreserved functions and enabled scheduling; it is not an emergency stop. Updates
are not transactional; keep backups.

For an optional emergency stop or teardown, stop callers first, verify the caller
identity and set `PREFIX` to the recorded deployment prefix (default `pi-durable`).
These commands need no build, Docker or Pulumi state. Stop all three functions and
disable scheduling:

```sh
for name in api worker reconciler; do
  aws --profile "$AWS_PROFILE" --region "$AWS_REGION" lambda put-function-concurrency \
    --function-name "$PREFIX-$name" --reserved-concurrent-executions 0
done
target="$(mktemp)"
aws --profile "$AWS_PROFILE" --region "$AWS_REGION" scheduler get-schedule \
  --name "$PREFIX-reconcile" --group-name "$PREFIX" --query Target --output json > "$target" &&
aws --profile "$AWS_PROFILE" --region "$AWS_REGION" scheduler update-schedule \
  --name "$PREFIX-reconcile" --group-name "$PREFIX" --state DISABLED \
  --schedule-expression 'rate(1 minute)' --flexible-time-window Mode=OFF --target "file://$target"
rm -f "$target"
```

Inspect every result; verify all three with `lambda get-function-concurrency` and the
schedule with `scheduler get-schedule`. Stop callers and drain/inspect in-flight and
queued work: these commands do not cancel running invocations or erase async queues.
When ready to resume, review an interactive `pulumi -C infra refresh`, then apply
known-good code to restore unreserved concurrency and enabled scheduling. Keep
callers stopped until recovery and schema inspection are complete.

Data and bucket controls have Pulumi protection/retention; DSQL also has deletion
protection. Destruction is a **separate approval**:

1. Stop callers, all three functions and scheduling as above. Drain/inspect in-flight
   and queued work, then back up data.
2. Set `allowDataDeletion=true`, review an interactive refresh and apply to remove
   safeguards. **This apply also restores normal compute and scheduling.** Keep
   callers stopped, then repeat the manual function/schedule stop, verify it and
   drain/inspect work again before deleting any data.
3. Manually remove **all object versions and delete markers**, including staging,
   and handle incomplete uploads. Verify every listing page is empty **before any
   `pulumi destroy`**. Otherwise destroy can remove bucket security controls before
   failing with `BucketNotEmpty`. Inspect and restore controls after any failure.

`forceDestroy` stays false for DSQL and S3; nothing empties S3 automatically.
If teardown is cancelled, set `allowDataDeletion=false` and apply to restore
safeguards; this may also restore compute, so repeat the manual stop if needed.
Backend deletion is a separate manual operation.
