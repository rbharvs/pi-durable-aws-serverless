/** Supported provider-neutral library entry. SQL composition is internal source-pinned wiring. */
export { apiHandler, type ApiDependencies, type SubmitResponse } from "./submit.ts";

export { harnessId, requestId, type HarnessId, type RequestId } from "./domain.ts";

export { canonical, parseCommand, type Command, type AgentInput } from "./input.ts";

export type { Authority, ApiAuthority, InboxItem, Received } from "./authority.ts";

export type { AcceptedInput } from "./submit.ts";

export type { Dispatch, Events } from "./dispatch.ts";
