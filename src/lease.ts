import type { HarnessId, OwnerToken, Epoch } from "./domain.ts";

export const LEASE_MARGIN_MS = 2000;

/** Stop admitting before the invocation deadline to join Pi and close storage. */
export const CLOSE_MARGIN_MS = 5000;

/** Adapter-internal issuance; only successful authority claims create leases. */
class IssuedLease {
  // oxlint-disable-next-line no-unused-private-class-members -- Private field supplies nominal lease identity.
  readonly #issued = true;

  constructor(
    readonly ns: HarnessId,
    readonly owner: OwnerToken,
    readonly epoch: Epoch,
    readonly until: number,
  ) {
    Object.freeze(this);
  }
}

/** Nominal, immutable authority-issued ownership. A copied structural value is not a lease. */
export type Lease = IssuedLease;

export function issueLease(ns: HarnessId, owner: OwnerToken, epoch: Epoch, until: number): Lease {
  return new IssuedLease(ns, owner, epoch, until);
}

export type Claim = { readonly kind: "claimed"; readonly lease: Lease } | { readonly kind: "held" };
