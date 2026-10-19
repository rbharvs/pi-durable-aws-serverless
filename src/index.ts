/**
 * Public entry point. It has no SQL or provider dependencies; the SQL adapter under `src/sql`
 * is internal wiring and is not exported.
 */
export { apiHandler, type ApiDependencies, type SubmitResponse } from "./submit.ts";

export { harnessId, requestId, type HarnessId, type RequestId } from "./domain.ts";

export { canonical, parseCommand, type Command, type AgentInput } from "./input.ts";

export type { AcceptedInput, Authority, ApiAuthority, InboxItem, Received } from "./authority.ts";

export type { Dispatch, Events } from "./dispatch.ts";
