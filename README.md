# Pi Durable on AWS Serverless

An example of [`@earendil-works/pi-durable`](https://www.npmjs.com/package/@earendil-works/pi-durable) targeting AWS Lambda and Amazon Aurora DSQL.

The current implementation provides a PostgreSQL-backed request receipt API. It accepts inline text and image input up to 256 KiB of canonical JSON and stores an immutable receipt for each Harness ID and request ID.

Retrying the same request with the same input returns the original receipt and timestamp. Different input for the same request returns a conflict without replacing the receipt. An uncertain database outcome is reported as retryable, not as a definite rejection.

Acceptance does not execute an agent. Worker execution and AWS deployment are not included. See the [admission integration tests](tests/integration/admission.test.ts) for request and response examples.

## Development

Requires Nix with flakes enabled. Integration tests also require a running Docker daemon.

Install dependencies:

```sh
nix develop --command pnpm install --frozen-lockfile
```

Check lint, formatting, and types:

```sh
nix develop --command pnpm check
```

Format files:

```sh
nix develop --command pnpm format
```

Run offline tests (no database or cloud services required):

```sh
nix develop --command pnpm test
```

Run PostgreSQL integration tests:

```sh
nix develop --command pnpm test:integration
```

The integration suite starts a temporary PostgreSQL 16 container on localhost and removes it afterward. It pulls a pinned image if needed. No AWS credentials are required.

## AI usage

AI coding agents contributed substantially to research, design, implementation, testing, and review. The maintainer set the requirements and made the final product and architectural decisions.

## License

[MIT](LICENSE)
