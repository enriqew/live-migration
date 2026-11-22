from migration.apply import Change, Offsets, Txn, newest, position, txn_key


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


def test_a_postgres_transaction_is_its_txid_whatever_lsn_each_record_carries():
    assert txn_key("dublin", "6337:13227552704") == txn_key("dublin", "6337:13227564568")
    gtid = "4d0375ab-e266-11f1-8a3a-ea187f2b8ab7:1015"
    assert txn_key("helsinki", gtid) == gtid


def test_a_marker_waits_for_what_committed_before_it_and_holds_back_what_came_after(monkeypatch):
    from migration import apply as ap

    batches = []  # (row ids applied, marker) per target transaction, in order

    def fake_apply_batch(conn, changes, offsets, applied_txns=("", []), marker=None):
        batches.append(([c.row_id for c in changes], marker))
        return {}

    monkeypatch.setattr(ap, "apply_batch", fake_apply_batch)
    s = ap.Service("helsinki", batch=100)
    s.target = None
    tp_data, tp_end, tp_marker = (
        ("helsinki.helsinki.matka", 0),
        ("helsinki.transaction", 0),
        (
            "helsinki.helsinki.verify_marker",
            0,
        ),
    )
    for o in range(10):
        for tp in (tp_data, tp_end, tp_marker):
            s.offsets.read(tp, o)

    def txn(name, end, rows=(), marker=None):
        t = Txn()
        t.expected = {"helsinki.verify_marker": 1} if marker else {"helsinki.matka": len(rows)}
        t.end_at = (tp_end, end)
        for i, r in enumerate(rows):
            t.changes.append((tp_data, end * 10 + i, change(r, (end, i))))
            t.seen["helsinki.matka"] += 1
        if marker:
            t.marker = marker
            t.meta.append((tp_marker, end))
            t.seen["helsinki.verify_marker"] += 1
        s.txns[name] = t
        return name

    m = ap.Marker("m1", "matka", 1, 100)
    # A ends before the marker, B after it. B completes first, then the marker; A is still
    # missing an event, so the marker must wait for it, and B must not be applied before it.
    a = txn("A", 1, rows=["a1", "a2"])
    s.txns[a].seen["helsinki.matka"] = 1
    txn("M", 2, marker=m)
    s.txns["M"].seen["helsinki.verify_marker"] = 0  # its END is read, its marker event not yet
    txn("B", 3, rows=["b1"])
    s._maybe_ready("B")  # B is complete before the marker is: it must still wait for it
    s.txns["M"].seen["helsinki.verify_marker"] = 1
    s._maybe_ready("M")
    assert batches == [] and s.ready == []  # nothing may go in yet
    s.txns[a].seen["helsinki.matka"] = 2  # A's last event arrives
    s._maybe_ready("A")
    s.flush()
    assert batches == [(["a1", "a2"], None), ([], m), (["b1"], None)]
