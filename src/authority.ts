import type { HarnessId, RequestId, Sha256 } from "./domain.ts";
import type { StoredPayload } from "./payload.ts";
import type { AcceptedInput } from "./submit.ts";

export type Received = {
  readonly kind: "received";
  readonly ns: HarnessId;
  readonly request: RequestId;
  readonly hash: Sha256;
  readonly payload: StoredPayload;
  readonly receivedAt: number;
};

export type InboxItem = Received;

/** Each method is one atomic authority operation. */
export interface Authority {
  get(ns: HarnessId, request: RequestId): Promise<InboxItem | undefined>;
  /** First immutable canonical winner. Same identity returns original receipt; conflict never replaces it. */
  receive(
    ns: HarnessId,
    request: RequestId,
    accepted: AcceptedInput,
  ): Promise<{ duplicate: boolean; item: InboxItem }>;
  /** Close this handle, never backing state. */
  close(): Promise<void>;
}

export type ApiAuthority = Pick<Authority, "get" | "receive" | "close">;
