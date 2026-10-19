import { type ApiAuthority, type InboxItem } from "./authority.ts";
import { RequestConflict, Unavailable, InvalidInput } from "./faults.ts";
import {
  type HarnessId,
  type RequestId,
  type Sha256,
  harnessId,
  object,
  requestId,
} from "./domain.ts";
import { canonical, type Command, parseCommand } from "./input.ts";
import { type StoredPayload } from "./payload.ts";
import { type Dispatch, type Events } from "./dispatch.ts";
import { emit, cleanup } from "./diagnostics.ts";

export type AcceptedInput = {
  readonly hash: Sha256;
  readonly payload: StoredPayload;
};

/** Canonical receipt acceptance. Acceptance is durable input, not execution. */
export async function admit(
  control: ApiAuthority,
  ns: HarnessId,
  request: RequestId,
  command: Command,
): Promise<{ duplicate: boolean; item: InboxItem }> {
  const c = canonical(command.input);
  let existing;

  try {
    existing = await control.get(ns, request);
  } catch (error) {
    throw new Unavailable(error, "receipt-read");
  }

  if (existing) {
    if (existing.hash !== c.sha256) throw new RequestConflict();

    return { duplicate: true, item: existing };
  }

  const payload: StoredPayload = { kind: "inline", json: c.json };

  try {
    return await control.receive(ns, request, { hash: c.sha256, payload });
  } catch (error) {
    if (error instanceof RequestConflict) throw error;

    throw new Unavailable(error, "receipt-write");
  }
}

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
  connectAuthority(): Promise<ApiAuthority>;
  readonly dispatch: Dispatch;
  readonly event: Events;
  /** Optional private diagnostic sink. Full causes never enter the wire or routine events. */
  report?(cause: unknown): void;
};

function parse(input: unknown) {
  try {
    const v = object(input);

    return {
      ns: harnessId(v.ns),
      request: requestId(v.request),
      command: parseCommand(v.command),
    };
  } catch (cause) {
    throw new InvalidInput(cause);
  }
}

function failure(error: unknown): Extract<SubmitResponse, { ok: false }> {
  if (error instanceof InvalidInput) return { ok: false, code: "invalid-input", retryable: false };
  if (error instanceof RequestConflict)
    return { ok: false, code: "request-conflict", retryable: false };

  return {
    ok: false,
    code: error instanceof Unavailable ? "unavailable" : "internal",
    retryable: true,
  };
}

/** Success means known durable receipt. No error asserts non-acceptance. Retry identical logical content only. */
export function apiHandler(deps: ApiDependencies) {
  return async (input: unknown): Promise<SubmitResponse> => {
    let control: ApiAuthority | undefined;

    try {
      const { ns, request, command } = parse(input);

      try {
        control = await deps.connectAuthority();
      } catch (error) {
        throw new Unavailable(error, "acquire");
      }

      const ack = await admit(control, ns, request, command);
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
        /* diagnostics never change an outcome */
      }

      return outcome;
    } finally {
      if (control) await cleanup(() => control!.close(), deps.event, "authority");
    }
  };
}
