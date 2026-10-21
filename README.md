# Pi Durable on AWS Serverless

An example of [`@earendil-works/pi-durable`](https://www.npmjs.com/package/@earendil-works/pi-durable) on AWS Lambda, Aurora DSQL, and S3. It provides immutable request receipts, a real Pi worker, missed-wake reconciliation, and complete Pi 1.3.0 storage. A memory backend supports local development.

The AWS adapters and handler compositions are tested offline and against local PostgreSQL, **not yet live-tested on AWS**. This repository does not provision infrastructure, authentication, or a scheduler.

SQL storage persists conversations, entries, tasks, submissions, and documents, with atomic commits, document history, and forks. An internal [backend factory](src/sql/backend.ts) pairs Authority and Storage; it is not a public package API.

## AWS composition

Use Node.js 24 and the pinned dependencies. The operator supplies a regional DSQL endpoint and schema, a private S3 bucket, execution-role permissions, Lambda functions, and periodic reconciler invocations.

| Environment variable                                       | Required by        |
| ---------------------------------------------------------- | ------------------ |
| `AWS_REGION`, `DSQL_HOST`, `DSQL_SCHEMA`                   | All three roots    |
| `PAYLOAD_BUCKET`                                           | API and worker     |
| `WORKER_NAME` (function name or ARN, optionally qualified) | API and reconciler |

DSQL connections sign as `admin` for the configured region and verify TLS. API and reconciler
can invoke the worker. The supplied worker composition does not import the project dispatcher;
operator tools may use Lambda independently. IAM, not bundle inspection, governs AWS authority.
Clients use the execution environment's credentials; never bundle credentials. Endpoint validation
currently accepts cluster hosts under `dsql.<region>.on.aws` with a two-letter first region label;
this is not a promise of support for every AWS partition or DSQL service availability.

Scope permissions to the configured resources: API needs DSQL connect-admin, S3 GET on `in/*`, S3 PUT+GET on `p/*`, and worker invocation; worker needs DSQL connect-admin and S3 PUT+GET on `p/*`; reconciler needs DSQL connect-admin and worker invocation. Without S3 ListBucket permission, a missing key may return 403, which remains retryable rather than confirmed missing.

`pnpm build` emits Node 24 CommonJS `dist/api.cjs` and `dist/reconciler.cjs`, with matching
`*-meta.json` files. Both handlers export `handler`. The deployer supplies a configured worker
entry using the pinned Pi 1.3.0:

```ts
import { createAwsHandler } from "./src/aws/worker.ts";
import { harnessOptions, rootAgent } from "./application.ts";

export const handler = createAwsHandler(harnessOptions, rootAgent);
```

`harnessOptions` supplies models and tools; `rootAgent` selects the agent. Run
`nix develop --command pnpm build -- path/to/worker.ts` to also write `dist/worker.cjs` and its
metadata, using the **same bytes checked by the build**. There is no default worker: without an
entry, the build checks the factory but removes stale worker outputs. These six named code/metadata
paths are build-owned; do not use them as source files. Other files in `dist` are left alone.

[Dependency checks](graphs.ts) exclude emitted SQL/AWS code from the public index, execution-engine
code from API/reconciler, S3 from reconciler, and the project dispatcher from worker. Tree-shaken
imports and operator Lambda tools are allowed. These checks do not certify behavior, permissions,
dynamic loads, or prebundled code. Builds never execute operator entries or certify their
`handler` export. Tests load actual CLI artifacts with [a controlled worker](tests/fixtures/aws-worker.ts);
operators must test their own entry and deploy the resulting bytes.

API and worker events are direct Lambda payloads, not API Gateway envelopes. Invoke the API with
`{ ns, request, command }`; advisory worker events carry `{ ns }` plus optional correlation fields.

**Provision DSQL manually before use.** [schema.sql](src/sql/schema.sql) is PostgreSQL DDL, not a ready-to-run DSQL migration. Prepare and review separate DSQL statements, run DDL outside explicit transactions, create indexes with `CREATE INDEX ASYNC`, and wait for successful jobs and valid indexes. Unsupported index clauses need explicit review, not silent SQL changes. Provisioning, IAM, deployment, DSQL DDL, and object-retention policies remain operator responsibilities.

## Usage

Each Harness ID and request ID identifies an immutable receipt. Retrying with the same canonical input returns the original receipt; different input returns a conflict.

- Inline commands: `{ kind: "inline", input: { content, whenBusy } }`.
- Staged commands: `{ kind: "s3", key, sha256, bytes }`, referencing an uploaded UTF-8 JSON file containing `{ content, whenBusy }`.

Staged keys use `in/<Harness ID>/<name>`, where the name is 1–128 ASCII letters, digits, or `._-`, excluding `.` and `..`. `sha256` and `bytes` describe the raw uploaded bytes, not canonical JSON. Multipart staging is supported: supply the full raw-object SHA-256, not S3's composite checksum. See [input limits](src/input.ts).

See the [inline](tests/integration/admission.test.ts) and [staged](tests/integration/staged-admission.test.ts) examples. Staged input requires an adapter implementing [`CommandObjects`](src/object-data.ts); the package does not upload staging files for the caller.

## Memory backend

Use `createMemoryBackend({ now?: () => number })` from [the public entry](src/index.ts) as the `backend` passed to `runtimeDependencies`. Omit `now` for the system clock; tests can supply a deterministic clock. Reuse the same backend to retain state across handle closes and worker invocations. See the [public consumer](src/memory-consumer.test.ts).

Memory uses Pi's public MemoryStorage and implements the same lease, receipt and allocation contracts. It is **not persistent**: losing the backend loses all state. Use SQL for durable storage. [Parity limits](tests/shared/PARITY.md) distinguish logical contracts from SQL transactions, object storage and physical budgets.

## Worker

Route the API's advisory `dispatch.wake(ns)` to `worker(ns, remainingMs, dependencies)`, where `remainingMs()` returns the invocation's remaining milliseconds. Build dependencies with `runtimeDependencies({ backend, harnessOptions, rootAgent, objects, event })`: `harnessOptions` supplies Pi models and a registry, and `objects(ns)` returns a reader for published payloads with `get` and `close`.

The worker claims the Harness, admits receipts to the root conversation, and runs Pi until it is idle or near the deadline. It returns `held`, `idle`, `released`, or `lost`, which describe ownership, not task completion. See the [worker example](tests/integration/worker.test.ts) and its [composition](tests/integration/worker-support.ts).

## Reconciliation

Invoke `reconcile({ connectAuthority, dispatch, event })` periodically to recover missed hints and expired owners. It closes Authority, then attempts wakes for at most five Harnesses with durable work, oldest lease expiry first. One failed wake does not stop the others. Its return value counts candidates, not confirmed deliveries or completed tasks. Hints may repeat; only the worker claims ownership. See the [reconciliation example](tests/integration/reconciler.test.ts).

Query cost may grow with stored namespaces and history; five oldest candidates whose wakes never lead to a claim can block later discovery indefinitely.

## Durability

If a storage operation has an unknown outcome, close and reopen its handle, then inspect durable state before deciding whether to retry. An error does not prove that nothing committed.

A worker closes Pi and its Storage before releasing ownership. On execution or close failure it does not release; a failed release may already have applied. Pi deduplicates admission and replays interrupted work, but external tool effects are not exactly-once. The close margin is not a hard timeout, so task code must honor cancellation. Missed hints and unfinished work need another invocation, directly or through reconciliation.

Published objects must remain immutable and available while referenced. S3 writes use random keys, conditional creation, and checksum confirmation; after an uncertain write, only matching HEAD metadata can confirm that same key. An unconfirmed error is not proof of absence. Failed publications may leave orphan objects; there is no automatic object or history cleanup. Storage resource bounds are defined in [limits](src/limits.ts).

## Development

Requires Nix with flakes enabled. Integration tests also require Docker and create and remove a temporary local PostgreSQL 16 container. No AWS credentials are needed.

```sh
# Install dependencies
nix develop --command pnpm install --frozen-lockfile

# Check lint, formatting, and types
nix develop --command pnpm check

# Run offline tests (including memory histories: two seeds × 50 cases)
nix develop --command pnpm test

# Run PostgreSQL integration tests
nix develop --command pnpm test:integration

# Check emitted dependency exclusions and build API/reconciler artifacts
nix develop --command pnpm build
```

The [SQL schema](src/sql/schema.sql) provisions a fresh database. Existing databases require a manual schema migration.

## AI usage

AI coding agents contributed substantially to research, design, implementation, testing, and review. The maintainer set the requirements and made the final product and architectural decisions.

## License

[MIT](LICENSE)
