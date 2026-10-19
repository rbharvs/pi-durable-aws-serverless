/** Internal typed causes. Public mapping never parses provider messages. */
export class InvalidInput extends Error {
  constructor(cause: unknown) {
    super("Invalid input", { cause });
  }
}

export class StagedContentInvalid extends Error {
  constructor(cause: unknown) {
    super("Staged content invalid", { cause });
  }
}

export class RequestConflict extends Error {
  constructor() {
    super("Request payload mismatch");
  }
}

export class Unavailable extends Error {
  constructor(
    cause: unknown,
    readonly phase:
      | "acquire"
      | "objects-acquire"
      | "staged-read"
      | "receipt-read"
      | "publish"
      | "receipt-write",
  ) {
    super("Unavailable", { cause });
  }
}
