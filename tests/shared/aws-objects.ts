import { Readable } from "node:stream";
import {
  S3Client,
  NoSuchKey,
  S3ServiceException,
  type PutObjectCommandInput,
  type GetObjectCommandInput,
} from "@aws-sdk/client-s3";
import { ImmutableObjects } from "../../src/aws/s3.ts";
import { check, computeSha256, type HarnessId } from "../../src/domain.ts";
import { stagedReference } from "../../src/input.ts";
import { aws } from "./aws.ts";

/** Actual SDK clients/commands with an offline terminal middleware, NOT an S3 service emulator. */
export function sdkObjects() {
  const bytes = new Map<string, Buffer>();
  const calls: Array<{ name: string; input: GetObjectCommandInput | PutObjectCommandInput }> = [];
  const streams: Readable[] = [];
  let handles = 0;
  return {
    bytes,
    calls,
    streams,
    openHandles: () => handles,
    stage(ns: HarnessId, value: string | Uint8Array, name = "input") {
      const body = Buffer.from(value);
      const ref = stagedReference(ns, {
        key: `in/${ns}/${name}`,
        sha256: computeSha256(body),
        bytes: body.length,
      });
      bytes.set(ref.key, body);
      return ref;
    },
    objects(ns: HarnessId) {
      const client = new S3Client({ ...aws, maxAttempts: 1 });
      handles++;
      const destroy = client.destroy.bind(client);
      client.destroy = () => {
        handles--;
        destroy();
      };
      client.middlewareStack.add(
        (_next, context) => async (args) => {
          const input = args.input as GetObjectCommandInput | PutObjectCommandInput;
          check(context.commandName, "Missing SDK command name");
          calls.push({ name: context.commandName, input });
          check(input.Bucket === "offline-payloads" && input.Key, "Unexpected object resource");
          if (context.commandName === "PutObjectCommand") {
            const put = input as PutObjectCommandInput;
            check(put.IfNoneMatch === "*", "Upload must be conditional");
            check(put.Body instanceof Uint8Array, "Expected buffered upload");
            const body = Buffer.from(put.Body);
            const checksum = Buffer.from(computeSha256(body), "hex").toString("base64");
            check(
              put.ChecksumSHA256 === checksum && put.ContentLength === body.length,
              "Upload integrity",
            );
            if (bytes.has(input.Key)) {
              throw new S3ServiceException({
                name: "PreconditionFailed",
                $fault: "client",
                $metadata: { httpStatusCode: 412 },
              });
            }
            bytes.set(input.Key, body);
            return { response: {}, output: { $metadata: {}, ChecksumSHA256: checksum } };
          }
          const body = bytes.get(input.Key);
          if (!body) {
            throw new NoSuchKey({ $metadata: { httpStatusCode: 404 }, message: "missing" });
          }
          const metadata = {
            ContentLength: body.length,
            ChecksumSHA256: Buffer.from(computeSha256(body), "hex").toString("base64"),
          };
          if (context.commandName === "HeadObjectCommand") {
            return { response: {}, output: { $metadata: {}, ...metadata } };
          }
          check(context.commandName === "GetObjectCommand", "Unexpected object command");
          const stream = Readable.from(
            (function* () {
              for (let i = 0; i < body.length; i += 16384) {
                yield Buffer.from(body.subarray(i, i + 16384));
              }
            })(),
          );
          streams.push(stream);
          return { response: {}, output: { $metadata: {}, ...metadata, Body: stream } };
        },
        { step: "initialize", name: "offlineObjects" },
      );
      return new ImmutableObjects("offline-payloads", ns, aws, client);
    },
  };
}
