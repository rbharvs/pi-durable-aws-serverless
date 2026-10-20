import type { SubmissionId } from "@earendil-works/pi-durable";
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

/** The original receipt with its durable submission mapping. */
export type Admitted = Omit<Received, "kind"> & {
  readonly kind: "admitted";
  readonly submission: SubmissionId;
};

export type InboxItem = Received | Admitted;

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
  /** At most eight unmapped receipts, oldest first. */
  received(ns: HarnessId): Promise<Received[]>;
  /** Maps a receipt to an existing submission; equal repeats are harmless, changes fail. */
  markAdmitted(received: Received, id: SubmissionId): Promise<void>;
  /** One authoritative snapshot of unmapped receipts and live task/submission records. */
  hasWork(ns: HarnessId): Promise<boolean>;
  /** Closes this handle. Stored receipts are unaffected. */
  close(): Promise<void>;
}

/** Receipt-only consumers do not need native history capabilities. */
export type ApiAuthority = Pick<Authority, "get" | "receive" | "close">;
