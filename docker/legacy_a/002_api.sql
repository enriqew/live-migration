-- The legacy API's idempotency record. Written in the same transaction as the change it guards,
-- so a retried request either sees the committed response or blocks until it exists.
-- Not part of the business data: the CDC connector does not capture it.
CREATE TABLE api_idempotency (
    key           text PRIMARY KEY,
    request_hash  text NOT NULL,
    response      jsonb,
    created_at    timestamptz NOT NULL DEFAULT now()
);
