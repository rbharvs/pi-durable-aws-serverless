# Agent guide

## Architecture

Keep one strict TypeScript package. Separate provider-neutral policy from SQL and AWS adapters, and pass narrow capabilities at explicit composition roots. Use public package exports and preserve a single Pi runtime identity.

Enforce invariants in authoritative state. Preserve transactional fencing, acknowledged durable allocation, atomic publication, immutable receipts, and uncertainty poisoning. Receipt acceptance is distinct from execution completion. Join actual Harness closure before releasing ownership.

## Development

Nix supplies Node, pnpm, and Git. pnpm owns dependencies and project commands. Keep exact dependency pins and committed lockfiles.

- Install: `nix develop --command pnpm install --frozen-lockfile`.
- Check formatting: `nix develop --command pnpm check`.
- Format files: `nix develop --command pnpm format` (mutates files).

The Nix shell is a tool environment, not a security sandbox.

## Code and tests

Use domain-specific names, small capability interfaces, and discriminated unions for distinct states. Validate untrusted values from `unknown`; brands do not replace runtime validation or authorization. Keep public exports deliberate and contracts explicit.

Keep acquisition, failure handling, and cleanup ordering visible. Preserve error causes and distinguish confirmed aborts from unknown outcomes. Retry only when the authoritative failure contract permits it. Comments should explain invariants, ownership, and non-obvious ordering.

Separate meaningful phases with vertical whitespace. Test observable behavior, persisted outcomes, and the absence of forbidden writes. Prefer deterministic operation gates over timing assumptions, and real adapter tests for database guarantees. Keep unhandled asynchronous failures visible and join owned work.

## Safety

Use synthetic providers and local resources for tests. Cloud, live-provider, and MCP execution; deployment; workstation changes; global installation; and credential-file inspection require explicit approval.

Keep secrets, credentials, runtime state, test output, and build output out of Git.
