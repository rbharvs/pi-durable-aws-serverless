import type { Events } from "./dispatch.ts";

/** Diagnostics are never authority or lifecycle controls. */
export function emit(event: Events, name: string, fields: Record<string, unknown>): void {
  try {
    event(name, fields);
  } catch {
    /* observer failure cannot change an outcome */
  }
}

export async function cleanup(
  close: () => void | Promise<void>,
  event: Events,
  resource: string,
): Promise<void> {
  try {
    await close();
  } catch {
    emit(event, "cleanup-failed", { resource });
  }
}
