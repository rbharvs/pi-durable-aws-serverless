/** TEST ONLY: valid deterministic PNG, exactly 4.5 MiB binary / 6 MiB base64. */
import { createHash } from "node:crypto";
import { deflateSync } from "node:zlib";

export const PNG_BYTES = 4.5 * 1024 * 1024;

const width = 1024;
const height = 1500;

function crc32(body: Buffer): number {
  let crc = 0xffffffff;

  for (const byte of body) {
    crc ^= byte;
    for (let i = 0; i < 8; i++) {
      crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0);
    }
  }

  return (crc ^ 0xffffffff) >>> 0;
}

function chunk(kind: string, body: Buffer): Buffer {
  const payload = Buffer.concat([Buffer.from(kind), body]);
  const size = Buffer.alloc(4);
  const crc = Buffer.alloc(4);

  size.writeUInt32BE(body.length);
  crc.writeUInt32BE(crc32(payload));

  return Buffer.concat([size, payload, crc]);
}

const pixels = createHash("shake256", { outputLength: width * height * 3 })
  .update("integration nonprivate PNG v1")
  .digest();

const scan = Buffer.alloc((width * 3 + 1) * height);

for (let y = 0; y < height; y++) {
  pixels.copy(scan, y * (width * 3 + 1) + 1, y * width * 3, (y + 1) * width * 3);
}

const ihdr = Buffer.alloc(13);

ihdr.writeUInt32BE(width);

ihdr.writeUInt32BE(height, 4);

ihdr[8] = 8;

ihdr[9] = 2;

const prefix = Buffer.concat([
  Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
  chunk("IHDR", ihdr),
  chunk("IDAT", deflateSync(scan)),
]);

const padding = PNG_BYTES - prefix.length - 24;

if (padding < 8) throw new Error("PNG fixture does not fit target");

const png = Buffer.concat([
  prefix,
  chunk("tEXt", Buffer.concat([Buffer.from("fixture\0"), Buffer.alloc(padding - 8, 120)])),
  chunk("IEND", Buffer.alloc(0)),
]);

if (png.length !== PNG_BYTES) throw new Error("PNG fixture size mismatch");

export const largeImage = png.toString("base64");

export const imageFixture = {
  pngBytes: png.length,
  base64Bytes: largeImage.length,
  width,
  height,
  pngSha256: createHash("sha256").update(png).digest("hex"),
  base64Sha256: createHash("sha256").update(largeImage).digest("hex"),
};
