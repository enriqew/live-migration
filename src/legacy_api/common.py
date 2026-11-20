"""What the two legacy APIs share: request hashing and the idempotency protocol.

Every write carries an `Idempotency-Key`. The API inserts the key before doing the work, inside
the same transaction:

- A new key inserts, the work runs, the response is stored, all of it commits together.
- A key already committed conflicts: the stored response is returned and nothing runs again.
- A key whose first request is still in flight: both engines block the second insert on the
  first transaction's row lock (PostgreSQL's `ON CONFLICT`, InnoDB's duplicate-key check), so the
  retry waits and then reads the committed response. No lock is taken by hand.
- The same key with a different body is a client bug, answered 422, never silently replayed.
- A request the business rules refuse (409) or that names nothing (404) rolls back with its key:
  nothing was written, so a retry is evaluated afresh.

This is what makes a retry during the cutover fence safe: a client that timed out does not know
whether its write landed, and sending it again must not write it twice.
"""

from __future__ import annotations

import hashlib
import json

from fastapi import HTTPException


def request_hash(method: str, path: str, body: dict | None) -> str:
    canonical = json.dumps(
        {"method": method, "path": path, "body": body or {}}, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def require_key(key: str | None) -> str:
    if not key:
        raise HTTPException(400, "Idempotency-Key header is required on every write")
    if len(key) > 64:
        raise HTTPException(400, "Idempotency-Key is at most 64 characters")
    return key


def replayed(stored_hash: str, stored_response, this_hash: str):
    if stored_hash != this_hash:
        raise HTTPException(422, "Idempotency-Key reused with a different request")
    if isinstance(stored_response, str):
        stored_response = json.loads(stored_response)
    return stored_response


class WriteGate:
    """Whether this legacy API still takes writes.

    Closed by the cutover once traffic has moved, opened again by a rollback. It guards the
    application, not the database: during the rollback window the reverse replication still has
    to write into the legacy databases, and a read-only database would stop it too. A write that
    finds the gate closed gets a 503, which a client retries, and the retry goes through the
    gateway to wherever the system now lives.
    """

    def __init__(self):
        self.open = True

    def check(self) -> None:
        if not self.open:
            raise HTTPException(503, "this system has moved; writes are closed here")


gate = WriteGate()


class Conflict(Exception):
    """A business rule refused the write (no bike to rent, no free dock). Answered 409."""
