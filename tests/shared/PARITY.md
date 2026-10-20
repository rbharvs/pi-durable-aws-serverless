# Backend parity

The shared Authority/Storage contracts and the unmodified public Pi 1.3.0
conformance assertions run against memory and actual PostgreSQL. Conformance
runs both live and with a fresh Storage handle after every commit. Existing SQL
fault, payload, worker, API and reconciliation tests remain separate and enabled.

Memory has no physical transactions or native transport uncertainty; SQL has
both. Shared assertions are unconditional. Memory fault decorators lose an
operation's acknowledgement before or after actual application; they test
caller recovery, not a memory network protocol. SQL faults still execute against
the real database.

## Shared invariants

- Expiry permits another claim; it does not revoke the current owner. Epoch,
  owner and commit sequence fence writes. IDs acknowledged by mint survive
  close, failed business batches and takeover. Exhaustion does not wrap.
- Receipts retain canonical identity, original bytes and timestamp. Mapping
  requires a stored submission in that namespace; it is immutable and remains
  possible after takeover.
- Commit inputs and read selectors detach at invocation; returned records are
  detached. Rejected batches publish no records or discovery changes. Closing
  a handle never destroys backing state.
- Work includes received receipts, pending/running/waiting/completing tasks and
  queued/placed submissions. Discovery is advisory, strictly expired, ordered
  and capped. Shared tests cover both ordering keys and limits.

## Implementation differences

Memory is process-local, not durable across process loss. The backend owns Pi
MemoryStorage independently of handles. Only public Pi exports are used.
Pi 1.3.0 public prepare/apply and complete reads are synchronous: the wrapper
must not await between fence validation, preparation, application and metadata
publication. A next-turn takeover regression protects that boundary.

Both adapters reuse the existing normalized JSON write-shape policy and logical
input-batch bound. Pi owns valid document behavior: global IDs, incarnations,
base/delta history, copy, retirement and fork semantics. Memory delegates that
engine rather than maintaining another implementation. SQL independently
validates stored rows and materializes documents, retaining old revisions even
for current-only documents. Pi may discard those revisions. Both prohibit
historical content reads on current-only documents.

Neither is a validator for arbitrary application document semantics. For
example, a structurally valid delta can replace an object with a scalar: Pi's
memory engine can return that scalar, while SQL rejects the materialized
non-object. Memory does not implement SQL's resolved-copy byte budget, row or
transaction-byte accounting, immutable object publication, corruption detection,
confirmed-abort retry, or transaction uncertainty. The memory input-batch cap
does not bound expanded copies or total retained state. Do not use it to
estimate SQL cost or AWS/DSQL deployability.

## Generated histories

Only memory runs the independent plain event ledger: two default seeds, 50
histories per seed, each with a mandatory prefix plus 1–20 generated operations.
The ledger checks returned IDs and actual stored entries, not implementation
fields. The mandatory prefix catches the mint-local mutant; generated tails
are not claimed as its unique detector. No generated PostgreSQL history claim
is made.

Replay with `HISTORY_SEED=20261017 HISTORY_INDEX=0` before the normal Nix/pnpm
test command for `src/memory-histories.test.ts`. Failures include the seed,
index and complete operation list. This small deterministic generator does
not shrink cases and needs no additional dependency. The standalone prefix
test always runs. The SQL suite supplies independent real-adapter fault coverage.
