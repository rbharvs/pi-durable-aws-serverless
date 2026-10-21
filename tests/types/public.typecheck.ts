import type { JsonValue } from "@earendil-works/chord";
import type { CanonicalInput } from "../../src/input.ts";
import type {
  AcceptedInput,
  Admitted,
  AgentInput,
  Events,
  InboxItem,
  PublishedPayload,
  Received,
  StagedReference,
  Authority,
  ApiAuthority,
  ApiDependencies,
  SubmitResponse,
  CommandObjects,
  WorkerAuthority,
  ReconcilerAuthority,
  ReconcilerDependencies,
  WorkerDependencies,
  WorkerResult,
  RuntimeOptions,
  Lease,
} from "../../src/index.ts";

type Assert<T extends true> = T;

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

export type AuthorityKeys = Assert<
  Equal<
    keyof Authority,
    | "get"
    | "receive"
    | "claim"
    | "release"
    | "received"
    | "markAdmitted"
    | "hasWork"
    | "eligible"
    | "close"
  >
>;
export type ReconcilerAuthorityKeys = Assert<
  Equal<keyof ReconcilerAuthority, "eligible" | "close">
>;
export type ReconcilerDependencyKeys = Assert<
  Equal<keyof ReconcilerDependencies, "connectAuthority" | "dispatch" | "event">
>;

export type ApiAuthorityKeys = Assert<Equal<keyof ApiAuthority, "get" | "receive" | "close">>;

export type ApiDependencyKeys = Assert<
  Equal<keyof ApiDependencies, "connectAuthority" | "objects" | "dispatch" | "event" | "report">
>;

export type WorkerAuthorityKeys = Assert<
  Equal<
    keyof WorkerAuthority,
    "claim" | "release" | "received" | "markAdmitted" | "hasWork" | "close"
  >
>;
export type WorkerDependencyKeys = Assert<
  Equal<keyof WorkerDependencies, "connectAuthority" | "open" | "resolve" | "event">
>;
export type WorkerOutcomes = Assert<Equal<WorkerResult, "held" | "idle" | "released" | "lost">>;
export type RuntimeReaderKeys = Assert<
  Equal<keyof ReturnType<RuntimeOptions["objects"]>, "get" | "close">
>;
export type WorkerOpenLease = Assert<Equal<Parameters<WorkerDependencies["open"]>[0], Lease>>;

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

/** Public data values can be logged directly and payload metadata can be stored as Pi JSON. */
export function publicDataConsumers(
  event: Events,
  accepted: AcceptedInput,
  received: Received,
  input: AgentInput,
  staged: StagedReference,
  published: PublishedPayload,
  canonical: CanonicalInput,
  admitted: Admitted,
  item: InboxItem,
) {
  event("accepted", accepted);
  event("received", received);
  event("input", input);
  event("staged", staged);
  event("published", published);
  event("canonical", canonical);
  event("admitted", admitted);
  event("item", item);

  const records: Array<Record<string, unknown>> = [
    accepted,
    received,
    input,
    staged,
    published,
    canonical,
    admitted,
    item,
  ];
  const metadata: JsonValue[] = [published, staged];

  return { records, metadata };
}

export function capabilities(a: Authority) {
  // @ts-expect-error No SQL escape in application authority.
  void a.db;
  // @ts-expect-error No provider escape in application authority.
  void a.provider;
}
