import { canonical, MAX_INLINE_INPUT_BYTES } from "../../src/input.ts";
import { check } from "../../src/domain.ts";
import type { AcceptedInput } from "../../src/submit.ts";

/** Constructs a bounded inline receipt for internal tests/callers after validation. */
export function inlineInput(value: unknown): AcceptedInput {
  const c = canonical(value);

  check(c.bytes <= MAX_INLINE_INPUT_BYTES, "Inline input exceeds 256 KiB");

  return { hash: c.sha256, payload: { kind: "inline", json: c.json } };
}

/** Defines an enumerable accessor that returns `first` on its first read and `later` afterwards. */
export function changing<T extends object>(
  target: T,
  key: string,
  first: unknown,
  later: unknown,
): T {
  let reads = 0;

  return Object.defineProperty(target, key, {
    enumerable: true,
    get: () => (reads++ === 0 ? first : later),
  });
}
