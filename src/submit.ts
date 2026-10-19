import { type AcceptedInput, type Authority } from "./authority.ts";
import { RequestConflict, Unavailable, InvalidInput } from "./faults.ts";
import { type HarnessId, type RequestId, harnessId, object, requestId } from "./domain.ts";
import { type CanonicalInput, canonicalCommand } from "./input.ts";
import { type Dispatch, type Events } from "./dispatch.ts";
import { emit, cleanup } from "./diagnostics.ts";

export type SubmitResponse =
  | {
      readonly ok: true;
      readonly duplicate: boolean;
      readonly receivedAt: number;
    }
  | {
      readonly ok: false;
      readonly code: "invalid-input" | "request-conflict";
      readonly retryable: false;
    }
  | {
      readonly ok: false;
      readonly code: "unavailable" | "internal";
      readonly retryable: true;
    };

export type ApiDependencies = {
  connectAuthority(): Promise<Authority>;
  readonly dispatch: Dispatch;
  readonly event: Events;
  /** Optional private diagnostic sink. Full causes never enter the wire or routine events. */
  report?(cause: unknown): void;
};

function parse(input: unknown): { ns: HarnessId; request: RequestId; canonical: CanonicalInput } {
  try {
    const body = object(input);

    return {
      ns: harnessId(body.ns),
      request: requestId(body.request),
      canonical: canonicalCommand(body.command),
    };
  } catch (cause) {
    throw new InvalidInput(cause);
  }
}

async function acquireAuthority(deps: ApiDependencies): Promise<Authority> {
  try {
    return await deps.connectAuthority();
  } catch (error) {
    throw new Unavailable(error, "acquire");
  }
}

/** Stores the receipt. Any failure other than a conflict has an unknown outcome. */
async function storeReceipt(
  authority: Authority,
  ns: HarnessId,
  request: RequestId,
  accepted: AcceptedInput,
): ReturnType<Authority["receive"]> {
  try {
    return await authority.receive(ns, request, accepted);
  } catch (error) {
    if (error instanceof RequestConflict) throw error;

    throw new Unavailable(error, "receipt-write");
  }
}

function failure(error: unknown): Extract<SubmitResponse, { ok: false }> {
  if (error instanceof InvalidInput) {
    return { ok: false, code: "invalid-input", retryable: false };
  }
  if (error instanceof RequestConflict) {
    return { ok: false, code: "request-conflict", retryable: false };
  }

  return {
    ok: false,
    code: error instanceof Unavailable ? "unavailable" : "internal",
    retryable: true,
  };
}

/**
 * Creates the submit handler. Each call validates the request, stores an immutable receipt,
 * closes the authority, and then sends an advisory wake.
 *
 * `ok: true` means the receipt is durable. `invalid-input` and `request-conflict` mean this
 * input was not stored. `unavailable` and `internal` mean the outcome is unknown: retry with
 * the same request ID and the same logical content (equal canonical JSON).
 */
export function apiHandler(deps: ApiDependencies) {
  return async (input: unknown): Promise<SubmitResponse> => {
    // Set while this call owns an open authority; `finally` closes it on early exit.
    let openAuthority: Authority | undefined;

    try {
      const { ns, request, canonical } = parse(input);
      const authority = await acquireAuthority(deps);

      openAuthority = authority;

      const ack = await storeReceipt(authority, ns, request, {
        hash: canonical.sha256,
        payload: { kind: "inline", json: canonical.json },
      });
      const outcome: SubmitResponse = {
        ok: true,
        duplicate: ack.duplicate,
        receivedAt: ack.item.receivedAt,
      };

      emit(deps.event, "durable-ack", {
        ns,
        request,
        duplicate: ack.duplicate,
      });

      // Close before the wake, so a slow wake never holds the connection.
      openAuthority = undefined;
      await cleanup(authority, deps.event, "authority");

      try {
        await deps.dispatch.wake(ns);
      } catch {
        emit(deps.event, "hint-failed", { ns });
      }

      return outcome;
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
      if (openAuthority) await cleanup(openAuthority, deps.event, "authority");
    }
  };
}
