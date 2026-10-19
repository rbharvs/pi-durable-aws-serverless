import type { HarnessId, RequestId, Sha256 } from "./domain.ts";
import type { StoredPayload } from "./payload.ts";

/**
 * Validated canonical Pi input, inline or already durably published, and its canonical SHA-256.
 * SQL validates reference metadata, not object existence. Publishers guarantee immutability.
 */
export type AcceptedInput = {
  readonly hash: Sha256;
  readonly payload: StoredPayload;
};

/** An immutable receipt for one request. */
export type Received = {
  readonly kind: "received";
  /** The Harness ID that scopes request IDs. */
  readonly ns: HarnessId;
  readonly request: RequestId;
  /** SHA-256 of the canonical JSON in `payload`. */
  readonly hash: Sha256;
  readonly payload: StoredPayload;
  /** Unix epoch milliseconds from the database clock, fixed when the receipt was first stored. */
  readonly receivedAt: number;
};

/** A stored inbox entry. Receipts are currently the only kind. */
export type InboxItem = Received;

/** Durable receipt storage. Each method is one atomic operation. */
export interface Authority {
  get(ns: HarnessId, request: RequestId): Promise<InboxItem | undefined>;
  /**
   * Stores the first receipt for `ns` and `request`, or returns the existing one.
   * An identical canonical input returns the original receipt with `duplicate: true`.
   * A different input throws `RequestConflict` and never replaces the original.
   */
  receive(
    ns: HarnessId,
    request: RequestId,
    accepted: AcceptedInput,
  ): Promise<{ duplicate: boolean; item: InboxItem }>;
  /** Closes this handle. Stored receipts are unaffected. */
  close(): Promise<void>;
}

/** The authority operations the submit handler uses; currently all of them. */
export type ApiAuthority = Pick<Authority, "get" | "receive" | "close">;
