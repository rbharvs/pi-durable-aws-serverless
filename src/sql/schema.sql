-- Provision once; no runtime DDL, no per-Harness tables, no foreign keys.
CREATE TABLE harness (
  ns text PRIMARY KEY, epoch bigint NOT NULL, owner uuid, lease_until bigint NOT NULL,
  next_seq bigint NOT NULL, next_id bigint NOT NULL
);

CREATE TABLE inbox (
  ns text NOT NULL,
  request_id text NOT NULL,
  payload_sha256 text NOT NULL,
  payload_kind text NOT NULL CHECK(payload_kind IN ('inline','s3')),
  json text,
  object_key text,
  object_bytes bigint,
  received_at bigint NOT NULL,
  submission_id bigint,
  PRIMARY KEY (ns, request_id),
  CHECK (
    (payload_kind='inline' AND json IS NOT NULL AND octet_length(json)<=262144 AND object_key IS NULL AND object_bytes IS NULL)
    OR (payload_kind='s3' AND json IS NULL AND object_key IS NOT NULL AND object_bytes IS NOT NULL AND object_bytes>262144 AND object_bytes<=8388608)
  )
);
CREATE INDEX inbox_received ON inbox(ns,submission_id,received_at,request_id);
CREATE INDEX inbox_pending ON inbox(submission_id,ns);
