import { type AcceptedInput, type ApiAuthority, type Received } from "./authority.ts";
import { RequestConflict, StagedContentInvalid, Unavailable, InvalidInput } from "./faults.ts";
import {
  check,
  computeSha256,
  harnessId,
  object,
  requestId,
  type HarnessId,
  type RequestId,
} from "./domain.ts";
import {
  canonicalStagedInput,
  MAX_INLINE_INPUT_BYTES,
  MAX_INPUT_BYTES,
  type AgentInput,
  prepareCommand,
  type CanonicalInput,
  type PreparedCommand,
} from "./input.ts";
import { publishedPayload, type StoredPayload } from "./payload.ts";
import { ObjectInvalid, readAtMost, type CommandObjects } from "./object-data.ts";
import { type Dispatch, type Events } from "./dispatch.ts";
import { emit, cleanup } from "./diagnostics.ts";
import type { PayloadObjects } from "./s3-payload.ts";

/** Reads only durable receipt bytes, never the caller's mutable staging object. */
export async function resolveInput(
  item: Received,
  objects: Pick<PayloadObjects, "get">,
): Promise<AgentInput> {
  const payload =
    item.payload.kind === "s3" ? publishedPayload(item.ns, item.payload) : item.payload;
  const json = payload.kind === "inline" ? payload.json : await objects.get(payload);
  const bytes = Buffer.byteLength(json);
  check(bytes <= MAX_INPUT_BYTES, "Inbox input exceeds bounded size");
  const input = canonicalStagedInput(Buffer.from(json));
  check(input.json === json && input.sha256 === item.hash, "Inbox payload integrity");
  if (payload.kind === "s3") {
    check(bytes === payload.bytes && input.sha256 === payload.sha256, "Inbox object integrity");
  }
  return input.input;
}

/** Receipt acknowledgment or classified failure, not confirmation that the agent processed input. */
export type SubmitResponse =
  | {
      readonly ok: true;
      readonly duplicate: boolean;
      readonly receivedAt: number;
    }
  | {
      readonly ok: false;
      readonly code: "invalid-input" | "request-conflict" | "staged-content-invalid";
      readonly retryable: false;
    }
  | {
      readonly ok: false;
      readonly code: "unavailable" | "internal";
      readonly retryable: true;
    };

/** Submission-owned handles are closed before the advisory wake; sinks remain caller-owned. */
export interface ApiDependencies {
  /** Each call returns a fresh handle closed by the submission, including on failure. */
  connectAuthority(): Promise<ApiAuthority>;
  /** Omit for inline-only admission. A handle is acquired only for staged commands. */
  objects?(ns: HarnessId): CommandObjects;
  readonly dispatch: Dispatch;
  readonly event: Events;
  /** Optional private diagnostic sink. Full causes never enter the wire or routine events. */
  report?(cause: unknown): void;
}

function parse(input: unknown): { ns: HarnessId; request: RequestId; command: PreparedCommand } {
  try {
    const body = object(input);
    const ns = harnessId(body.ns);
    const request = requestId(body.request);
    const command = prepareCommand(body.command, ns);

    return { ns, request, command };
  } catch (cause) {
    throw new InvalidInput(cause);
  }
}

async function acquireAuthority(deps: ApiDependencies): Promise<ApiAuthority> {
  try {
    return await deps.connectAuthority();
  } catch (error) {
    throw new Unavailable(error, "acquire");
  }
}

function acquireObjects(deps: ApiDependencies, ns: HarnessId): CommandObjects {
  if (!deps.objects) throw new InvalidInput(new Error("Staged input is not configured"));

  try {
    return deps.objects(ns);
  } catch (error) {
    throw new Unavailable(error, "objects-acquire");
  }
}

async function resolveStaged(
  objects: CommandObjects,
  command: Extract<PreparedCommand, { kind: "s3" }>,
): Promise<CanonicalInput> {
  // Fix the declared integrity before the adapter receives the reference.
  const { bytes, sha256 } = command;
  let raw: Buffer | undefined;

  try {
    raw = await readAtMost(await objects.getStaged(command), bytes);
  } catch (error) {
    if (error instanceof ObjectInvalid) throw new StagedContentInvalid(error);

    throw new Unavailable(error, "staged-read");
  }

  try {
    check(
      raw?.length === bytes && computeSha256(raw) === sha256,
      "Staged object integrity mismatch",
    );

    return canonicalStagedInput(raw);
  } catch (error) {
    throw new StagedContentInvalid(error);
  }
}

/** Reads an existing receipt; any failure is an unavailable read. */
async function findReceipt(authority: ApiAuthority, ns: HarnessId, request: RequestId) {
  try {
    return await authority.get(ns, request);
  } catch (error) {
    throw new Unavailable(error, "receipt-read");
  }
}

async function publish(
  objects: CommandObjects,
  ns: HarnessId,
  input: CanonicalInput,
): Promise<StoredPayload> {
  try {
    const payload = publishedPayload(ns, await objects.put(input.json));

    check(
      payload.sha256 === input.sha256 && payload.bytes === input.bytes,
      "Publication identity mismatch",
    );

    return payload;
  } catch (error) {
    throw new Unavailable(error, "publish");
  }
}

/** Stores the receipt. Any failure other than a conflict has an unknown outcome. */
async function storeReceipt(
  authority: ApiAuthority,
  ns: HarnessId,
  request: RequestId,
  accepted: AcceptedInput,
): ReturnType<ApiAuthority["receive"]> {
  try {
    return await authority.receive(ns, request, accepted);
  } catch (error) {
    if (error instanceof RequestConflict) throw error;

    throw new Unavailable(error, "receipt-write");
  }
}

/**
 * Publishes input too large for an inline receipt, then stores its receipt. The prior read avoids
 * an orphan publication when the request already has a receipt.
 */
async function receivePublished(
  authority: ApiAuthority,
  objects: CommandObjects,
  ns: HarnessId,
  request: RequestId,
  input: CanonicalInput,
): ReturnType<ApiAuthority["receive"]> {
  const existing = await findReceipt(authority, ns, request);

  if (existing) {
    if (existing.hash !== input.sha256) throw new RequestConflict();

    return { duplicate: true, item: existing };
  }

  const payload = await publish(objects, ns, input);

  return storeReceipt(authority, ns, request, { hash: input.sha256, payload });
}

function failure(error: unknown): Extract<SubmitResponse, { ok: false }> {
  if (error instanceof InvalidInput) {
    return { ok: false, code: "invalid-input", retryable: false };
  }
  if (error instanceof RequestConflict) {
    return { ok: false, code: "request-conflict", retryable: false };
  }
  if (error instanceof StagedContentInvalid) {
    return { ok: false, code: "staged-content-invalid", retryable: false };
  }

  return {
    ok: false,
    code: error instanceof Unavailable ? "unavailable" : "internal",
    retryable: true,
  };
}

/**
 * Validates input, publishes large canonical JSON before its immutable receipt, closes owned
 * resources, then sends an advisory wake. Concurrent losers and uncertain writes may leave orphans.
 *
 * `ok: true` means the receipt is durable. Nonretryable responses mean this invocation did not
 * store its input; they say nothing about earlier/concurrent submissions of the same request ID.
 * `unavailable` and `internal` mean the outcome is unknown: retry with the same request ID and
 * logical content (equal canonical JSON), using a currently valid staged reference when needed.
 */
export function apiHandler(deps: ApiDependencies) {
  return async (input: unknown): Promise<SubmitResponse> => {
    let ns: HarnessId;
    let ack: Awaited<ReturnType<ApiAuthority["receive"]>>;
    // Resources opened by this call. `finally` closes them before the wake.
    let authority: ApiAuthority | undefined;
    let objects: CommandObjects | undefined;

    try {
      const submission = parse(input);
      const request = submission.request;
      let canonicalInput: CanonicalInput;

      ns = submission.ns;
      if (submission.command.kind === "inline") canonicalInput = submission.command.canonical;
      else {
        objects = acquireObjects(deps, ns);
        canonicalInput = await resolveStaged(objects, submission.command);
      }

      authority = await acquireAuthority(deps);

      if (canonicalInput.bytes <= MAX_INLINE_INPUT_BYTES) {
        const payload = { kind: "inline", json: canonicalInput.json } as const;

        ack = await storeReceipt(authority, ns, request, { hash: canonicalInput.sha256, payload });
      } else {
        check(objects, "Only staged input can exceed the inline limit");
        ack = await receivePublished(authority, objects, ns, request, canonicalInput);
      }

      emit(deps.event, "durable-ack", { ns, request, duplicate: ack.duplicate });
    } catch (error) {
      const outcome = failure(error);

      emit(deps.event, "submit-failed", {
        code: outcome.code,
        phase: error instanceof Unavailable ? error.phase : "validation-or-internal",
      });

      try {
        deps.report?.(error);
      } catch {
        // Report failures are deliberately ignored; diagnostics never change an outcome.
      }

      return outcome;
    } finally {
      // Close before the wake, so a slow wake never holds a connection.
      if (authority) await cleanup(authority, deps.event, "authority");
      if (objects) await cleanup(objects, deps.event, "objects");
    }

    try {
      await deps.dispatch.wake(ns);
    } catch {
      emit(deps.event, "hint-failed", { ns });
    }

    return { ok: true, duplicate: ack.duplicate, receivedAt: ack.item.receivedAt };
  };
}
