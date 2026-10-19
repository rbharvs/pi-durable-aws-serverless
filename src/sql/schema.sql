-- Provision once; no runtime DDL.
CREATE TABLE inbox (
  ns text NOT NULL,
  request_id text NOT NULL,
  payload_sha256 text NOT NULL,
  json text NOT NULL CHECK(octet_length(json)<=262144),
  received_at bigint NOT NULL,
  PRIMARY KEY (ns, request_id)
);
