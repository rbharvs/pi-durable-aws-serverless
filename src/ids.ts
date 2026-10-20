// Adapted from Pi packages/durable/src/ids.ts at ae7b16d43b7c78c5b6fa3213fdd1215bb339729d.
// Copyright (c) 2025 Mario Zechner. MIT; see LICENSES/pi-MIT.txt.
import type { Id, Seq } from "@earendil-works/pi-durable";

/** Apply an erased ID brand at a trusted numeric allocation or decoding boundary. */
export function idFromNumber<I extends Id<string>>(value: number): I {
  return value as I;
}

/** Apply the erased commit-sequence brand at a trusted storage boundary. */
export function seqFromNumber(value: number): Seq {
  return value as Seq;
}
