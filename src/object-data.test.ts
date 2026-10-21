import { Readable } from "node:stream";
import { expect, it } from "vitest";
import { readAtMost } from "./object-data.ts";

it("returns exact bytes across chunks and releases the stream", async () => {
  const bytes = Buffer.from(' {"content":"é🚀"} ');
  const body = Readable.from([...bytes].map((byte) => Buffer.from([byte])));

  expect(await readAtMost(body, bytes.length)).toEqual(bytes);
  expect(body.destroyed).toBe(true);
});

it("returns a short stream for the caller to reject", async () => {
  expect(await readAtMost(Readable.from([Buffer.from("{")]), 2)).toEqual(Buffer.from("{"));
});

it("stops at the first byte over the limit without reading the rest", async () => {
  let reachedTail = false;
  const body = Readable.from(
    (async function* () {
      yield Buffer.from("x".repeat(10));
      yield Buffer.from("y".repeat(10));
      reachedTail = true;
      yield Buffer.from("z".repeat(10));
    })(),
    { highWaterMark: 1 },
  );

  expect(await readAtMost(body, 5)).toBeUndefined();
  expect(reachedTail).toBe(false);
  expect(body.destroyed).toBe(true);
});

it("isolates an asynchronous destruction error after stopping early", async () => {
  const body = new Readable({
    read() {
      this.push(Buffer.from("too long"));
    },
    destroy(_error, callback) {
      setImmediate(() => callback(new Error("private cleanup failure")));
    },
  });
  const closed = new Promise<void>((resolve) => body.once("close", resolve));

  expect(await readAtMost(body, 1)).toBeUndefined();
  // No test-owned error listener: a cleanup error escaping the reader fails the suite.
  await closed;
  expect(body.destroyed).toBe(true);
}, 1000);

it("copies chunks, so a producer cannot change bytes after they are read", async () => {
  const chunk = Buffer.from("{}");
  const body = (async function* () {
    yield chunk;
    chunk.fill(0);
    yield Buffer.alloc(0);
  })();

  expect((await readAtMost(body, 2))?.toString()).toBe("{}");
});

it("propagates transport failures unchanged", async () => {
  const cause = new Error("private transport failure");
  const body = Readable.from(
    (async function* () {
      yield Buffer.from("{");
      throw cause;
    })(),
  );

  await expect(readAtMost(body, 2)).rejects.toBe(cause);
  expect(body.destroyed).toBe(true);
});

it("rejects a text-mode stream as an adapter error, not as invalid content", async () => {
  const body = Readable.from(["{}"]);

  await expect(readAtMost(body, 2)).rejects.toBeInstanceOf(TypeError);
  expect(body.destroyed).toBe(true);
});
