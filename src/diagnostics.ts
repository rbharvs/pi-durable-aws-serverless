import type { Events } from "./dispatch.ts";

/** Sends a diagnostic event and ignores observer failures, so an observer cannot change an outcome. */
export function emit(event: Events, name: string, fields: Record<string, unknown>): void {
  try {
    event(name, fields);
  } catch {
    // Observer failures are deliberately ignored.
  }
}

/** Closes `resource` and reports a failure as a `cleanup-failed` event instead of throwing. */
export async function cleanup(
  resource: { close(): void | Promise<void> },
  event: Events,
  name: string,
): Promise<void> {
  try {
    await resource.close();
  } catch {
    emit(event, "cleanup-failed", { resource: name });
  }
}
