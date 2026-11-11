from carscraper.planner import Slice
from carscraper.store import Store


def _rec(i, country="Germania"):
    return {"listing_id": f"id{i}", "country": country, "price_euro": i}


def test_meta_round_trip(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    assert st.get_meta("makes") is None
    st.set_meta("makes", [9, 13])
    assert st.get_meta("makes") == [9, 13]


def test_new_slices_need_their_first_page(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    assert not st.has_slices()
    st.add_slices([Slice("D"), Slice("I")])
    assert st.has_slices()
    assert sorted(st.load_work(), key=lambda w: w[0].country) == [(Slice("D"), [1]), (Slice("I"), [1])]


def test_paging_slice_resumes_with_pages_not_done(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    s = Slice("D", 74)
    st.add_slices([s])
    st.set_slice_result(s, total=50, pages=3, state="paging")
    st.mark_page_done(s, 1)
    st.mark_page_done(s, 3)
    assert st.load_work() == [(s, [2])]


def test_split_and_finished_slices_have_no_work(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    a, b = Slice("D"), Slice("D", 9)
    st.add_slices([a, b])
    st.set_slice_result(a, total=900000, pages=200, state="split")
    st.set_slice_result(b, total=10, pages=1, state="paging")
    st.mark_page_done(b, 1)
    assert st.load_work() == []


def test_adding_existing_slice_keeps_its_progress(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    s = Slice("D", 9)
    st.add_slices([s])
    st.set_slice_result(s, total=10, pages=1, state="paging")
    st.add_slices([s])
    st.mark_page_done(s, 1)
    assert st.load_work() == []


def test_listings_are_saved_deduplicated_and_exported_in_order(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    st.save_listings([("id1", _rec(1), {"raw": 1}), ("id2", _rec(2, "Italia"), {"raw": 2})])
    st.save_listings([("id1", _rec(99), {"raw": 99})])  # duplicate ignored
    assert st.missing(["id1", "id2", "id3"]) == {"id3"}
    assert [r["listing_id"] for r in st.iter_records()] == ["id1", "id2"]
    assert next(st.iter_records())["price_euro"] == 1
    assert st.count() == 2
    assert st.count_by_country() == {"Germania": 1, "Italia": 1}
    assert st.get_raw("id2") == {"raw": 2}


def test_state_survives_reopen(tmp_path):
    path = tmp_path / "db.sqlite"
    st = Store(path)
    st.add_slices([Slice("NL")])
    st.save_listings([("id1", _rec(1), {})])
    st.close()
    st2 = Store(path)
    assert st2.load_work() == [(Slice("NL"), [1])]
    assert st2.count() == 1


def test_basic_listings_are_upgraded_by_full_ones(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    st.save_basic([("id1", "slug-1", _rec(1)), ("id2", "slug-2", _rec(2, "Italia"))])
    st.save_basic([("id1", "slug-1", _rec(50))])  # seen again: ignored
    assert st.count_by_level() == {"basic": 2}
    assert st.missing(["id1", "id3"]) == {"id1", "id3"}  # basic still needs details
    st.save_listings([("id1", {**_rec(1), "color": "Negru"}, {"raw": 1})])
    assert st.count_by_level() == {"basic": 1, "full": 1}
    assert st.missing(["id1", "id2"]) == {"id2"}
    assert next(st.iter_records())["color"] == "Negru"
    st.save_basic([("id1", "slug-1", _rec(1))])  # never downgrades
    assert st.count_by_level() == {"basic": 1, "full": 1}


def test_basic_after_pages_through_one_country_in_order(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    st.save_basic([(f"id{i}", f"s{i}", _rec(i, "Italia" if i % 2 else "Germania")) for i in range(6)])
    st.save_listings([("id2", _rec(2), {})])
    rows = st.basic_after("Germania", 0, limit=10)
    assert [(r[1], r[2]) for r in rows] == [("id0", "s0"), ("id4", "s4")]
    assert st.basic_after("Germania", rows[0][0], limit=10)[0][1] == "id4"
    assert rows[0][3] == _rec(0, "Germania")  # the saved basic record comes back too


def test_gone_listings_leave_the_details_queue(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    st.save_basic([("id1", "s1", _rec(1))])
    st.mark_gone(["id1"])
    assert st.basic_after("Germania", 0, 10) == []
    assert st.count_by_level() == {"gone": 1}
    assert [r["listing_id"] for r in st.iter_records()] == []


def test_only_full_export_filter(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    st.save_basic([("id1", "s1", _rec(1))])
    st.save_listings([("id2", _rec(2), {})])
    assert [r["listing_id"] for r in st.iter_records()] == ["id1", "id2"]
    assert [r["listing_id"] for r in st.iter_records(only_full=True)] == ["id2"]


def test_old_database_without_level_column_is_migrated(tmp_path):
    import sqlite3
    path = tmp_path / "old.sqlite"
    db = sqlite3.connect(path)
    db.executescript("""CREATE TABLE listings (id INTEGER PRIMARY KEY, listing_id TEXT NOT NULL UNIQUE,
        country TEXT, record BLOB NOT NULL, raw BLOB, fetched_at TEXT NOT NULL DEFAULT (datetime('now')));
        INSERT INTO listings (listing_id, country, record) VALUES ('old1', 'Italia', '{"listing_id":"old1"}');""")
    db.commit()
    db.close()
    st = Store(path)
    assert st.count_by_level() == {"full": 1}
    assert st.missing(["old1"]) == set()


def test_reset_crawl_restarts_search_but_keeps_listings(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    s = Slice("D", 9)
    st.add_slices([s])
    st.set_slice_result(s, total=10, pages=1, state="paging")
    st.mark_page_done(s, 1)
    st.save_basic([("id1", "s1", _rec(1))])
    st.reset_crawl()
    assert st.load_work() == []
    assert not st.has_slices()
    assert st.count() == 1


def test_save_attrs_inserts_and_upgrades_basic_but_not_full(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    st.save_basic([("id1", "s1", {**_rec(1), "first_seen_at": "T0"})])
    st.save_listings([("id2", {**_rec(2), "color": "Negru"}, {})])
    new = {**_rec(1), "first_seen_at": "T9", "color": "Alb", "body_type": "Combi", "doors": 5}
    added = st.save_attrs([("id1", "s1", new),
                           ("id2", "s2", {**_rec(2), "color": "Alb"}),
                           ("id3", "s3", {**_rec(3), "color": "Gri"})])
    assert added == 1  # only id3 is a new listing
    recs = {r["listing_id"]: r for r in st.iter_records()}
    assert recs["id1"]["color"] == "Alb" and recs["id1"]["doors"] == 5
    assert recs["id1"]["first_seen_at"] == "T0"  # keeps when it was first seen
    assert recs["id2"]["color"] == "Negru"  # full record untouched
    assert st.count_by_level() == {"attrs": 2, "full": 1}
    assert st.missing(["id1", "id3"]) == {"id1", "id3"}  # still need the listing page for features


def test_attrs_listings_feed_details_and_can_be_gone(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    st.save_attrs([("id1", "s1", _rec(1)), ("id2", "s2", _rec(2))])
    assert [r[1] for r in st.basic_after("Germania", 0, 10)] == ["id1", "id2"]
    st.mark_gone(["id1"])
    assert [r[1] for r in st.basic_after("Germania", 0, 10)] == ["id2"]
    st.save_listings([("id2", _rec(2), {})])
    assert st.count_by_level() == {"full": 1, "gone": 1}


def test_min_19_fields_filter(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    st.save_basic([("id1", "s1", _rec(1))])
    st.save_attrs([("id2", "s2", _rec(2))])
    st.save_listings([("id3", _rec(3), {})])
    assert [r["listing_id"] for r in st.iter_records(min_19=True)] == ["id2", "id3"]
