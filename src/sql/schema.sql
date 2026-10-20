-- Provision once; no runtime DDL.
CREATE TABLE harness (
  ns text PRIMARY KEY, epoch bigint NOT NULL, owner uuid, lease_until bigint NOT NULL,
  next_seq bigint NOT NULL, next_id bigint NOT NULL
);
CREATE TABLE records (
  ns text NOT NULL, id bigint NOT NULL, type text NOT NULL, payload_kind text NOT NULL CHECK(payload_kind IN ('inline','s3')), payload_ref uuid NOT NULL,
  conversation_id bigint, owner_conversation_id bigint, owner_task_id bigint,
  kind_hash text, status text, abort_requested boolean, background boolean,
  request_hash text, has_head boolean, commit_seq bigint,
  address_hash text, scope_kind text, scope_owner bigint, created_at bigint, retired_at bigint,
  PRIMARY KEY(ns,id)
);
CREATE TABLE revisions (
  ns text NOT NULL, document_id bigint NOT NULL, seq bigint NOT NULL,
  kind text NOT NULL, version double precision NOT NULL, payload_kind text NOT NULL CHECK(payload_kind IN ('inline','s3')), payload_ref uuid NOT NULL,
  PRIMARY KEY(ns,document_id,seq)
);
CREATE INDEX records_type ON records(ns,type,id);
CREATE INDEX records_conversation ON records(ns,type,conversation_id,id);
CREATE INDEX records_status ON records(ns,type,status,id);
CREATE INDEX records_kind ON records(ns,type,kind_hash,id);
CREATE INDEX records_head ON records(ns,conversation_id,has_head,id);
CREATE INDEX records_request ON records(ns,conversation_id,request_hash);
CREATE INDEX records_scope ON records(ns,scope_kind,scope_owner,id);
CREATE INDEX records_address ON records(ns,address_hash,created_at);
CREATE TABLE inline_payloads (
  ns text NOT NULL, payload_ref uuid NOT NULL, json text NOT NULL,
  PRIMARY KEY(ns,payload_ref)
);
CREATE TABLE s3_payloads (
  ns text NOT NULL, payload_ref uuid NOT NULL, object_key text NOT NULL,
  sha256 text NOT NULL, bytes bigint NOT NULL CHECK(bytes >= 0 AND bytes <= 16777216),
  PRIMARY KEY(ns,payload_ref)
);

CREATE TABLE inbox (
  ns text NOT NULL,
  request_id text NOT NULL,
  payload_sha256 text NOT NULL,
  payload_kind text NOT NULL,
  json text,
  object_key text,
  object_bytes integer,
  received_at bigint NOT NULL,
  submission_id bigint,
  PRIMARY KEY (ns, request_id),
  CHECK (
    (payload_kind='inline' AND json IS NOT NULL AND octet_length(json)<=262144
      AND object_key IS NULL AND object_bytes IS NULL)
    OR
    (payload_kind='s3' AND json IS NULL AND object_key IS NOT NULL
      AND object_bytes IS NOT NULL AND object_bytes>262144 AND object_bytes<=8388608)
  )
);
-- Unmapped receipts, oldest first: received() and WORK. Mapped receipts accumulate.
CREATE INDEX inbox_unmapped ON inbox(ns,received_at,request_id) WHERE submission_id IS NULL;
