import { canonical, MAX_INLINE_INPUT_BYTES } from "../../src/input.ts";
import { check } from "../../src/domain.ts";
import type { AcceptedInput } from "../../src/authority.ts";

/** Validates Pi input and returns the inline receipt input that `Authority.receive` accepts. */
export function acceptedInput(value: unknown): AcceptedInput {
  const c = canonical(value);

  check(c.bytes <= MAX_INLINE_INPUT_BYTES, "Inline input exceeds 256 KiB");

  return { hash: c.sha256, payload: { kind: "inline", json: c.json } };
}

/** Defines an enumerable accessor on `target` that returns `first` on its first read and `later` afterwards. */
export function withChangingProperty<T extends object>(
  target: T,
  key: string,
  values: { first: unknown; later: unknown },
): T {
  let reads = 0;

  return Object.defineProperty(target, key, {
    enumerable: true,
    get: () => (reads++ === 0 ? values.first : values.later),
  });
}
