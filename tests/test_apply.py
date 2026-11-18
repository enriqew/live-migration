from migration.apply import Change, Offsets, Txn, newest, position


def change(row_id, pos, kind="upsert"):
    return Change("trip", row_id, pos, kind, {}, ("helsinki", "matka", str(row_id)))


def test_the_highest_position_wins_whatever_the_arrival_order():
    batch = [change(1, (5, 0)), change(1, (9, 0), "delete"), change(1, (7, 2)), change(2, (1, 0))]
    latest = newest(batch)
    assert latest[("trip", 1)].pos == (9, 0) and latest[("trip", 1)].kind == "delete"
    assert latest[("trip", 2)].pos == (1, 0)


def test_minor_position_breaks_ties_within_one_major():
    latest = newest([change(1, (5, 3)), change(1, (5, 7)), change(1, (5, 1))])
    assert latest[("trip", 1)].pos == (5, 7)


def test_offsets_commit_only_up_to_the_lowest_unapplied():
    o = Offsets()
    tp = ("dublin.public.rental", 0)
    for off in (10, 11, 12, 13):
        o.read(tp, off)
    o.finished(tp, 10)
    o.finished(tp, 12)  # 11 still waits for its transaction
    assert o.committable() == {tp: 11}
    o.finished(tp, 11)
    o.finished(tp, 13)
    assert o.committable() == {tp: 14}


def test_positions_order_as_debezium_orders_them():
    pg = {"sequence": '["13227547312","13227552704"]', "lsn": 13227552704}
    assert position("dublin", pg) == (13227547312, 13227552704)
    a = position("helsinki", {"file": "binlog.000007", "pos": 662, "row": 0})
    b = position("helsinki", {"file": "binlog.000008", "pos": 4, "row": 0})
    c = position("helsinki", {"file": "binlog.000007", "pos": 662, "row": 1})
    assert a < c < b  # a rotated binlog file is later, whatever its position


def test_a_transaction_is_complete_only_with_every_counted_event():
    t = Txn()
    t.seen["public.rental"] = 1
    assert not t.complete()  # no END yet
    t.expected = {"public.rental": 1, "public.station_reading": 1}
    assert not t.complete()
    t.seen["public.station_reading"] = 1
    assert t.complete()
