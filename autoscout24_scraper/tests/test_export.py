import json

from carscraper.export import export
from carscraper.store import Store


def _store(tmp_path, n):
    st = Store(tmp_path / "db.sqlite")
    recs = [{"listing_id": f"id{i}", "make": "Citroën", "features": ["ABS", "ESP"], "country": "Italia"}
            for i in range(n)]
    st.save_listings([(r["listing_id"], r, None) for r in recs])
    return st, recs


def test_json_export_matches_cars_json_formatting(tmp_path):
    st, recs = _store(tmp_path, 3)
    out = tmp_path / "out.json"
    assert export(st, out) == 3
    text = out.read_text(encoding="utf-8")
    assert text == json.dumps(recs, ensure_ascii=False, indent=4)


def test_empty_export_is_valid_json(tmp_path):
    st, _ = _store(tmp_path, 0)
    out = tmp_path / "out.json"
    assert export(st, out) == 0
    assert json.loads(out.read_text()) == []


def test_jsonl_export_has_one_record_per_line(tmp_path):
    st, recs = _store(tmp_path, 2)
    out = tmp_path / "out.jsonl"
    export(st, out, fmt="jsonl")
    assert [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()] == recs


def test_export_min_19_fields(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    st.save_basic([("b", "s", {"listing_id": "b", "country": "Italia"})])
    st.save_attrs([("a", "s", {"listing_id": "a", "country": "Italia"})])
    out = tmp_path / "o.json"
    assert export(st, out, min_19=True) == 1


def test_export_stays_valid_json_with_unicode_line_separators(tmp_path):
    st = Store(tmp_path / "db.sqlite")
    rec = {"listing_id": "x", "version": "A B\x85C D\x0bE", "country": "Italia"}
    st.save_listings([("x", rec, None)])
    out = tmp_path / "o.json"
    export(st, out)
    assert json.loads(out.read_text(encoding="utf-8")) == [rec]
