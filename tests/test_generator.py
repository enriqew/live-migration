from generator.ops import OpStream, World


def world():
    return World(
        dublin_stations=[1, 2, 3], helsinki_stations=["043", "A04"], helsinki_max_trip_id=10
    )


def test_same_seed_same_answers_same_operations():
    a, b = OpStream(7, "run", world()), OpStream(7, "run", world())
    ops_a = [(o.kind, o.path, o.body, o.key) for o in (a.next() for _ in range(200))]
    ops_b = [(o.kind, o.path, o.body, o.key) for o in (b.next() for _ in range(200))]
    assert ops_a == ops_b


def test_keys_are_unique_and_differ_between_runs():
    a, b = OpStream(7, "run-1", world()), OpStream(7, "run-2", world())
    keys_a = {a.next().key for _ in range(500)}
    keys_b = {b.next().key for _ in range(500)}
    assert len(keys_a) == 500 and not keys_a & keys_b


def test_returns_and_erases_carry_the_rental_they_took():
    w = world()
    w.open_rentals.extend(range(100, 150))
    stream = OpStream(3, "run", w, share_dublin=1.0)
    taken = [o for o in (stream.next() for _ in range(300)) if o.kind in ("return", "erase")]
    assert taken and all(o.rental is not None and str(o.rental) in o.path for o in taken)
