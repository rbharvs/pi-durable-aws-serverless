# Pi Durable on AWS Serverless

An example of [`@earendil-works/pi-durable`](https://www.npmjs.com/package/@earendil-works/pi-durable) on AWS Lambda, Aurora DSQL, and S3. It provides immutable request receipts, a real Pi worker, missed-wake reconciliation, and complete Pi 1.3.0 storage. A memory backend supports local development.

Validation includes deployed Lambda/Pi workflows and direct DSQL/S3 contracts,
including controlled failure cases. Model fixtures are deterministic; these tests
do not establish arbitrary-outage tolerance or exactly-once external effects.
[Local Pulumi deployment](infra/README.md) provisions the AWS infrastructure, direct
IAM-authenticated Lambda invocation and a one-minute reconciliation schedule.
After one-time setup, `pnpm run deploy` runs checks/tests, builds all three Lambdas
including the supplied production worker, then runs interactive Pulumi. Deployment
is manual; GitHub Actions only validates. Callers select OpenAI or Anthropic models
at runtime; there is no default model.

SQL storage persists conversations, entries, tasks, submissions, and documents, with atomic commits, document history, and forks. An internal [backend factory](src/sql/backend.ts) pairs Authority and Storage; it is not a public package API.

## AWS composition

Use Node.js 24 and the pinned dependencies. The [deployment runbook](infra/README.md)
covers protected regional DSQL/S3 storage, schema initialization, execution roles, three
Lambdas and scheduling. The [production worker](src/aws/worker-entry.ts) uses Pi's
public registry and official OpenAI Responses / Anthropic Messages implementations.
Provider keys come from the worker environment, never source or bundles. Inputs
select the model; Pi persists it for continuation. The roots can also be composed
with independently managed infrastructure.

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

`pnpm run deploy` builds the production entry automatically. To build without deploying:

```sh
nix develop --command pnpm build -- src/aws/worker-entry.ts
```

This emits Node 24 CommonJS `dist/api.cjs`, `dist/reconciler.cjs` and `dist/worker.cjs`,
each exporting `handler`, with matching `*-meta.json` files. The packaged bytes are
the **same bytes checked by the build**. For custom applications, `createAwsHandler`
remains available; an optional `WORKER_ENTRY` deployment environment override selects
your tested entry. No override is needed for the supplied worker.

The low-level `pnpm build` command without an entry still checks the factory and
removes stale worker outputs; use the explicit entry above for a complete build.
These six named code/metadata paths are build-owned; do not use them as source files.
Other files in `dist` are left alone.

[Dependency checks](graphs.ts) exclude emitted SQL/AWS code from the public index, execution-engine
code from API/reconciler, S3 from reconciler, and the project dispatcher from worker. Tree-shaken
imports and operator Lambda tools are allowed. These checks do not certify behavior, permissions,
dynamic loads, or prebundled code. Builds never execute operator entries or certify their
`handler` export. Tests load the production CLI artifact, exercise its official
provider adapters as bundled CJS against synthetic HTTP streams, with selected
Node network entry points replaced in-process (not an OS sandbox),
and retain [controlled worker](tests/fixtures/aws-worker.ts) packaging tests. Custom
entry owners must test their own handler.

API and worker events are direct Lambda payloads, not API Gateway envelopes. Invoke the API with
`{ ns, request, command }`; advisory worker events carry `{ ns }` plus optional correlation fields.

**Initialize DSQL before submitting API requests.** Lambdas use the normal account
concurrency pool without dedicated reservations; the one-minute schedule is enabled.
After the first apply, the [deployment runbook](infra/README.md) initializes
[schema.sql](src/sql/schema.sql) once, using `CREATE INDEX ASYNC` and waiting for each
job. Scheduled reconciliation may briefly fail during this first bootstrap until
the schema is ready. Existing or partially initialized schemas need manual review,
not bootstrap replay. No SQL runs during Pulumi preview or apply.

## Usage

Each Harness ID and request ID identifies an immutable receipt. Retrying with the same canonical input returns the original receipt; different input returns a conflict.

- Inline commands: `{ kind: "inline", input: { content, whenBusy, model? } }`.
- Staged commands: `{ kind: "s3", key, sha256, bytes }`, referencing uploaded UTF-8 JSON
  containing the same input shape. Model selection is part of the immutable receipt
  hash. Reusing a request ID with a different model is a conflict.

For example, invoke the API with an explicit selection:

```json
{
  "ns": "example-conversation",
  "request": "message-001",
  "command": {
    "kind": "inline",
    "input": {
      "content": "Explain durable conversations.",
      "whenBusy": "followUp",
      "model": { "provider": "openai", "modelId": "gpt-4.1" }
    }
  }
}
```

For Anthropic, use `"model": { "provider": "anthropic", "modelId": "claude-sonnet-4-5" }`.
These are examples from the pinned Pi 1.3.0 chat catalogs, **not defaults**. For new
receipts, provider IDs are exactly `openai` and `anthropic`; model IDs must match their
[public Pi catalogs](src/model-selection.ts). Catalog membership does not guarantee
your provider account has access. No provider request runs during API validation.

- The first input must select a model. Later inputs may omit it to use the persisted
  choice, including after a cold start, or supply another supported model.
- The API acknowledges a durable receipt, **not an answer**. Missing an initial
  model settles that input as `unanswered` with reason `model_required`; send a new
  request ID with a selection to proceed. Invalid provider/model selections are
  rejected at admission (`invalid-input`, or `staged-content-invalid` for S3).
  Provider authentication/access failures remain Pi execution failures, not API
  admission failures. Answers and submission outcomes are stored in Pi history;
  this example does not add a result-query endpoint.
- A model change waits until the current Pi run finishes. It does not retarget an
  active run, even with `whenBusy: "steer"`. After a failed run, Pi may be idle with
  queued inputs that have not started. The next input lets Pi drain those inputs
  under the newly selected model; no queued messages are discarded. Later receipts
  wait behind a change while a run is active; a Lambda close margin leaves them
  durable for the next worker. Same-model steering/follow-up retains Pi's normal behavior.
  Receipt order is
  `receivedAt`, then request ID for ties, not a promise of client send order.
- The worker uses Pi's public transactional `configure` helper, then submits the
  message. On recovery it checks Pi's request identity **before** configuring;
  replay cannot restore an older model after the submission already exists.
  Old receipts without `model` keep their original canonical bytes/hashes.
- `canonical()` and `parseCommand()` validate stable structure and identity, not
  current catalog membership. Storage and worker reads remain valid when a model
  leaves the catalog. A retry of that accepted input still returns `duplicate`;
  changed input still conflicts. A new unsupported selection is rejected (the API
  may read a receipt to distinguish a retry). If the worker no longer has the model,
  Pi settles that input `unanswered` / `no_model`, rather than blocking receipt reads.

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
