import type {
  Authority,
  ApiAuthority,
  ApiDependencies,
  SubmitResponse,
  CommandObjects,
} from "../../src/index.ts";

type Assert<T extends true> = T;

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

export type AuthorityKeys = Assert<
  Equal<keyof Authority, "get" | "receive" | "received" | "markAdmitted" | "hasWork" | "close">
>;
export type ApiAuthorityKeys = Assert<Equal<keyof ApiAuthority, "get" | "receive" | "close">>;

export type ApiDependencyKeys = Assert<
  Equal<keyof ApiDependencies, "connectAuthority" | "objects" | "dispatch" | "event" | "report">
>;

export type ObjectCapabilityKeys = Assert<
  Equal<keyof CommandObjects, "getStaged" | "put" | "close">
>;

export type WireCodes = Assert<
  Equal<
    Extract<SubmitResponse, { ok: false }>["code"],
    "invalid-input" | "request-conflict" | "staged-content-invalid" | "unavailable" | "internal"
  >
>;

export type RetryableCodes = Assert<
  Equal<Extract<SubmitResponse, { retryable: true }>["code"], "unavailable" | "internal">
>;

export function capabilities(a: Authority) {
  // @ts-expect-error No SQL escape in application authority.
  void a.db;
  // @ts-expect-error No provider escape in application authority.
  void a.provider;
}
