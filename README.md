# Pi Durable on AWS Serverless

An example of [`@earendil-works/pi-durable`](https://www.npmjs.com/package/@earendil-works/pi-durable) targeting AWS Lambda and Amazon Aurora DSQL. Currently, it runs locally on PostgreSQL and provides a request receipt API, a worker that runs real Pi over those receipts, a reconciler for missed hints, and a complete Pi 1.3.0 `Storage` implementation. A public in-memory backend supports local development and tests; AWS adapters are not yet included.

SQL storage persists conversations, entries, tasks, submissions, and documents, with atomic commits, document history, and forks. An internal [backend factory](src/sql/backend.ts) pairs Authority and Storage; it is not a public package API.

## Usage

Each Harness ID and request ID identifies an immutable receipt. Retrying with the same canonical input returns the original receipt; different input returns a conflict.

- Inline commands: `{ kind: "inline", input: { content, whenBusy } }`.
- Staged commands: `{ kind: "s3", key, sha256, bytes }`, referencing an uploaded UTF-8 JSON file containing `{ content, whenBusy }`.

Staged keys use `in/<Harness ID>/<name>`, where the name is 1–128 ASCII letters, digits, or `._-`, excluding `.` and `..`. `sha256` and `bytes` describe the raw uploaded bytes, not canonical JSON. See [input limits](src/input.ts).

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

Published objects must remain immutable and available while referenced. Failed publications may leave orphan objects; there is no automatic object or history cleanup. Storage resource bounds are defined in [limits](src/limits.ts).

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
```

The [SQL schema](src/sql/schema.sql) provisions a fresh database. Existing databases require a manual schema migration.

## AI usage

AI coding agents contributed substantially to research, design, implementation, testing, and review. The maintainer set the requirements and made the final product and architectural decisions.

## License

[MIT](LICENSE)
