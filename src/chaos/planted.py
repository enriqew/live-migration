"""The apply service with a bug planted in the transform, for chaos scenario 10.

python -m chaos.planted helsinki

One trip in 101 (by legacy id) gets its distance multiplied by 1,000, the kind of slip a unit
conversion makes: metres taken for kilometres. Everything else is the real service. The point of
the scenario is not the bug; it is whether the verifier, which shares no code with this, names
exactly the rows and the column the bug touched, and nothing else.
"""

from __future__ import annotations

import sys

from migration import apply
from migration import transform as tf

PLANT_EVERY = 101

_real = tf.helsinki_trip


def planted_helsinki_trip(row: dict):
    out = _real(row)
    if isinstance(out, dict) and row["id"] % PLANT_EVERY == 0 and out["distance_m"] is not None:
        out = {**out, "distance_m": out["distance_m"] * 1000}
    return out


tf.helsinki_trip = planted_helsinki_trip

if __name__ == "__main__":
    sys.argv = ["migration.apply", *sys.argv[1:]]
    apply.main()
