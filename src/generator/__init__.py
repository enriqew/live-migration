"""The load generator: the users of both legacy systems, writing through their APIs.

Open loop. Operation i is due at `start + i / rate`, whatever happened to operation i - 1, and
its latency is measured from when it was due, not from when it was sent. A closed-loop generator
(send, wait, send) slows down exactly when the system does, and so stops measuring the pauses a
migration is supposed to prove short: coordinated omission. Here a stalled API shows up as a queue
of late operations, each of them counted.

Seeded, within a limit: the next operation depends on the seed and on what the systems
answered so far (a rental can only be returned once it was accepted, and whether a station had a
bike depends on timing). Given the same answers, the same seed gives the same operations; the log
records every answer, so a run can be explained after the fact even where it cannot be replayed.

Every operation keeps one idempotency key across all its attempts. A timeout, a refused
connection or a 503 is retried with the same key until a deadline, so a write the client could
not confirm is sent again without any risk of landing twice.
"""
