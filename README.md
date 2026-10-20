# Pi Durable on AWS Serverless

An example of [`@earendil-works/pi-durable`](https://www.npmjs.com/package/@earendil-works/pi-durable) targeting AWS Lambda and Amazon Aurora DSQL.

Currently, this provides a PostgreSQL-backed request receipt API. It accepts inline input or a reference to uploaded input. Agent execution and AWS integration are not included yet.

## Request receipts

Each Harness ID and request ID identifies one immutable receipt. Retrying with the same canonical input returns the original receipt and timestamp. Different input returns a conflict without replacing it.

Uncertain writes are retryable, not definite rejections. Retry with the same logical input and a valid staging reference when needed. An earlier or concurrent attempt may already have stored a receipt, even if the current attempt fails.

## Input

Inline commands use `{ kind: "inline", input: { content, whenBusy } }`.

Staged commands use `{ kind: "s3", key, sha256, bytes }`. The uploaded file contains the same `{ content, whenBusy }` object as UTF-8 JSON. Its key is `in/<Harness ID>/<name>`, where the name uses 1–128 letters, digits, `.`, `_`, or `-`, excluding `.` and `..`. The lowercase hexadecimal `sha256` and numeric `bytes` describe the raw uploaded bytes, not the canonical JSON.

Limits:

- Inline input: 256 KiB of canonical JSON.
- Staged input: 8 MiB of raw bytes and 8 MiB of canonical JSON.
- Content: up to 64 text/image blocks, including at most 8 images. Each decoded image is limited to 6 MiB and must also fit the total JSON limit.
- Staged JSON structure: depth at most 3 and at most 1024 colons/commas outside strings. Discarded duplicate-key values count toward these allocation limits.

Staged input requires an `ApiDependencies.objects(ns)` adapter implementing [`CommandObjects`](src/object-data.ts). Inline-only callers need no object adapter. The package does not upload staging files for the caller.

Small canonical inputs stay inline in PostgreSQL. Larger inputs are published as independent immutable objects before their receipt is stored. Receipts never depend on mutable staging data. The adapter must keep published objects unchanged and available for the receipt's lifetime. Failed or competing publications may leave orphan objects; automatic cleanup is not provided.

See the [inline](tests/integration/admission.test.ts) and [staged](tests/integration/staged-admission.test.ts) integration tests for complete examples.

## Internal conversation history

The source-pinned [SQL history adapter](src/sql/storage.ts) stores conversations and entries
atomically, with durable ID allocation, owner/epoch/sequence fencing, fork-aware scans, and
cold-reopen reads. Receipt admission creates the namespace; SQL authority claims use strict
database-clock lease expiry. Expiry alone does not invalidate the current owner's writes.

History commits detach caller data before queuing and budget all SQL/payload work before
uploading or beginning the write transaction. [History limits](src/limits.ts) are separate from
the 8 MiB command input limit above. Large values, or values selected under transaction byte
pressure, use immutable objects through an injected adapter. Uploads are bounded and joined on
failure. Failed publication may leave object orphans, but no partially committed database history.

Only provider-confirmed transaction aborts are retried. Unknown allocation or write-COMMIT
outcomes fail the handle and reject queued operations. Close and reopen, then read durable
state before deciding whether to retry; do not assume rejection. Reads validate payload
envelopes and indexed fields.

This is partial internal history storage, not a complete Pi `Storage`, `Backend`, or runtime.
Tasks, submissions, documents, agent execution, AWS adapters, and object cleanup are not included.

## Development

Requires Nix with flakes enabled. PostgreSQL integration tests also require a running Docker daemon; they create and remove a temporary PostgreSQL 16 container on localhost. No AWS credentials are required.

```sh
# Install dependencies
nix develop --command pnpm install --frozen-lockfile

# Check lint, formatting, and types
nix develop --command pnpm check

# Run offline tests
nix develop --command pnpm test

# Run PostgreSQL integration tests
nix develop --command pnpm test:integration
```

To format files, run `nix develop --command pnpm format`.

The [SQL schema](src/sql/schema.sql) provisions a fresh database. Existing databases require a manual schema migration.

## AI usage

AI coding agents contributed substantially to research, design, implementation, testing, and review. The maintainer set the requirements and made the final product and architectural decisions.

## License

[MIT](LICENSE)
