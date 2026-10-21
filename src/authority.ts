import type { Storage, SubmissionId } from "@earendil-works/pi-durable";
import type { HarnessId, RequestId, Sha256 } from "./domain.ts";
import type { Lease, Claim } from "./lease.ts";
import type { StoredPayload } from "./payload.ts";

/**
 * Validated canonical Pi input, inline or already durably published, and its canonical SHA-256.
 * SQL validates reference metadata, not object existence. Publishers guarantee immutability.
 */
export interface AcceptedInput {
  readonly hash: Sha256;
  readonly payload: StoredPayload;
}

/** An immutable receipt for one request. */
export interface Received {
  readonly kind: "received";
  /** The Harness ID that scopes request IDs. */
  readonly ns: HarnessId;
  readonly request: RequestId;
  /** SHA-256 of the canonical JSON in `payload`. */
  readonly hash: Sha256;
  readonly payload: StoredPayload;
  /** Unix epoch milliseconds from the authority clock, fixed when the receipt was first stored. */
  readonly receivedAt: number;
}

/** The original receipt with its durable submission mapping. */
export type Admitted = Omit<Received, "kind"> & {
  readonly kind: "admitted";
  readonly submission: SubmissionId;
};

/** A receipt stays immutable except for its monotonic mapping to an admitted submission. */
export type InboxItem = Received | Admitted;

/** Receipt storage. Each method is atomic; backing lifetime is adapter-specific. */
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
  /** Claims only a strictly expired lease; remainingMs is the invocation budget, not a renewal. */
  claim(ns: HarnessId, remainingMs: number): Promise<Claim>;
  /** Release only after joined Storage close; rejects uncertain writes rather than promising release. */
  release(lease: Lease): Promise<"released" | "lost">;
  /** At most eight unmapped receipts, oldest first. */
  received(ns: HarnessId): Promise<Received[]>;
  /** Maps a receipt to an existing submission; equal repeats are harmless, changes fail. */
  markAdmitted(received: Received, id: SubmissionId): Promise<void>;
  /** One authoritative snapshot of unmapped receipts and live task/submission records. */
  hasWork(ns: HarnessId): Promise<boolean>;
  /**
   * At most five Harness IDs with authoritative work and a strictly expired lease by authority time.
   * Ordered by lease expiry, then Harness ID. Advisory only: discovery does not claim ownership.
   */
  eligible(): Promise<HarnessId[]>;
  /** Closes this handle. Stored receipts are unaffected. */
  close(): Promise<void>;
}

/** Worker ownership and receipt admission, without receipt creation. */
export type WorkerAuthority = Pick<
  Authority,
  "claim" | "received" | "markAdmitted" | "hasWork" | "release" | "close"
>;

/** Pairing of one authority store and its fenced native Storage. */
export interface Backend {
  connectAuthority(): Promise<Authority>;
  /** Returned Storage.close must tolerate repeated calls, including after failed Harness.open. */
  openStorage(lease: Lease): Promise<Storage>;
}

/** Discovery only: a reconciler cannot claim ownership or admit receipts. */
export type ReconcilerAuthority = Pick<Authority, "eligible" | "close">;

/** Receipt-only consumers do not need native history capabilities. */
export type ApiAuthority = Pick<Authority, "get" | "receive" | "close">;
