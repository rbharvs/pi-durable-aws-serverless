import { randomUUID } from "node:crypto";
import { addAbortSignal, finished, Readable } from "node:stream";
import {
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
  NoSuchKey,
} from "@aws-sdk/client-s3";
import { bucketName, region, REQUEST_MS, transport, type AwsOptions } from "./config.ts";
import { check, computeSha256, harnessId, type HarnessId } from "../domain.ts";
import { jsonText, type PublishedPayload } from "../payload.ts";
import { stagedReference, type StagedReference } from "../input.ts";
import { objectSize, reference } from "../s3-payload.ts";
import { ObjectInvalid, readAtMost, type CommandObjects } from "../object-data.ts";

/** A fresh, owned client per handle. No mutable cache, overwrite, or unbounded SDK retries. */
export class ImmutableObjects implements CommandObjects {
  private readonly client: Pick<S3Client, "send" | "destroy">;
  private readonly active = new Map<AbortController, () => void>();
  private closed = false;

  constructor(
    private readonly bucket: string,
    private readonly ns: HarnessId,
    options: AwsOptions,
    client?: Pick<S3Client, "send" | "destroy">,
  ) {
    bucketName(bucket);
    harnessId(ns);
    const configuredRegion = region(options.region);
    this.client =
      client ??
      new S3Client({
        region: configuredRegion,
        credentials: options.credentials,
        maxAttempts: 1,
        requestHandler: transport,
      });
  }

  private operation() {
    check(!this.closed, "Object handle closed");
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), REQUEST_MS);
    timer.unref();
    const finish = () => {
      // Close, setup failure and stream completion can race; release the operation once.
      if (!this.active.delete(controller)) return;
      clearTimeout(timer);
    };
    this.active.set(controller, finish);
    return { signal: controller.signal, finish };
  }

  async put(json: string): Promise<PublishedPayload> {
    // Bound before allocating the upload buffer. The same random key is used for confirmation.
    const bytes = objectSize(json);
    const body = Buffer.from(json);
    const ref = reference(
      { object_key: `p/${this.ns}/${randomUUID()}`, sha256: computeSha256(body), bytes },
      this.ns,
    );
    const checksum = Buffer.from(ref.sha256, "hex").toString("base64");
    const operation = this.operation();
    try {
      try {
        const result = await this.client.send(
          new PutObjectCommand({
            Bucket: this.bucket,
            Key: ref.key,
            Body: body,
            ContentLength: bytes,
            IfNoneMatch: "*",
            ChecksumSHA256: checksum,
          }),
          { abortSignal: operation.signal },
        );
        check(result.ChecksumSHA256 === checksum, "S3 did not confirm checksum");
      } catch (error) {
        // An uncertain PUT may already have published. HEAD can confirm this exact immutable key;
        // failure to confirm is NOT proof of absence and never permits overwriting or a new PUT.
        try {
          const head = await this.client.send(
            new HeadObjectCommand({
              Bucket: this.bucket,
              Key: ref.key,
              ChecksumMode: "ENABLED",
            }),
            { abortSignal: operation.signal },
          );
          check(
            head.ContentLength === bytes && head.ChecksumSHA256 === checksum,
            "S3 publication unconfirmed",
          );
        } catch {
          throw error;
        }
      }
      return ref;
    } finally {
      operation.finish();
    }
  }

  async get(input: PublishedPayload): Promise<string> {
    const ref = reference(
      { object_key: input.key, sha256: input.sha256, bytes: input.bytes },
      this.ns,
    );
    const raw = await readAtMost(await this.read(ref), ref.bytes);
    check(
      raw?.length === ref.bytes && computeSha256(raw) === ref.sha256,
      "Object integrity mismatch",
    );
    return jsonText(new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(raw));
  }

  getStaged(input: StagedReference): Promise<AsyncIterable<Uint8Array>> {
    return this.read(stagedReference(this.ns, input));
  }

  /**
   * Return the backpressured SDK body under the operation deadline. resolveStaged/get own the
   * readAtMost byte bound and caller hash; SDK checksum failures remain transport failures.
   */
  private async read(ref: StagedReference | PublishedPayload): Promise<AsyncIterable<Uint8Array>> {
    const operation = this.operation();
    let body: Readable | undefined;
    try {
      const result = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: ref.key,
          ChecksumMode: "ENABLED",
        }),
        { abortSignal: operation.signal },
      );
      check(result.Body instanceof Readable, "Missing Node object body");
      body = result.Body;
      // Observe errors before addAbortSignal can destroy an already-aborted body. Iteration
      // still receives the original error, even if nobody was consuming when it occurred.
      const removeListeners = finished(body, () => {
        removeListeners();
        operation.finish();
      });
      addAbortSignal(operation.signal, body);
      if (result.ContentLength !== undefined && result.ContentLength !== ref.bytes) {
        throw new ObjectInvalid(new Error("Object length mismatch"));
      }
      return body;
    } catch (error) {
      body?.destroy();
      operation.finish();
      if (error instanceof NoSuchKey) throw new ObjectInvalid(error);
      throw error;
    }
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const [controller, finish] of this.active) {
      controller.abort();
      finish();
    }
    this.client.destroy();
  }
}

export function objectFactory(options: AwsOptions) {
  const bucket = bucketName(process.env.PAYLOAD_BUCKET);
  region(options.region);
  return (ns: HarnessId) => new ImmutableObjects(bucket, ns, options);
}
