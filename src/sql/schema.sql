-- Provision once; no runtime DDL.
CREATE TABLE inbox (
  ns text NOT NULL,
  request_id text NOT NULL,
  payload_sha256 text NOT NULL,
  payload_kind text NOT NULL,
  json text,
  object_key text,
  object_bytes integer,
  received_at bigint NOT NULL,
  PRIMARY KEY (ns, request_id),
  CHECK (
    (payload_kind='inline' AND json IS NOT NULL AND octet_length(json)<=262144
      AND object_key IS NULL AND object_bytes IS NULL)
    OR
    (payload_kind='s3' AND json IS NULL AND object_key IS NOT NULL
      AND object_bytes IS NOT NULL AND object_bytes>262144 AND object_bytes<=8388608)
  )
);
