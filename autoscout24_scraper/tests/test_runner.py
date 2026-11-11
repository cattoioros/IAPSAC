import copy

from carscraper.planner import Slice
from carscraper.runner import Runner
from carscraper.store import Store

MAKES = [9, 13]


class FakeClient:
    """Serves a fake catalogue: {slice_key: total listings}; listing ids are '<key>#<n>'."""

    def __init__(self, totals, details_template, gone=()):
        self.totals = totals
        self.template = details_template
        self.gone = set(gone)
        self.search_calls = []
        self.detail_calls = []
        self.order = []  # "search" / "details", in call order

    async def bootstrap(self):
        return MAKES

    async def search(self, params):
        key = Slice(params["cy"],
                    int(params["mmmv"].split("|")[0]) if "mmmv" in params else None).key
        page = params["page"]
        self.search_calls.append((key, page))
        self.order.append("search")
        total = self.totals.get(key, 0)
        start = (page - 1) * 20
        ids = [f"{key}#{n}" for n in range(start, min(start + 20, total, 4000))]
        iso = {"D": "DE", "I": "IT", "L": "LU", "NL": "NL"}[params["cy"]]
        return {
            "numberOfResults": total,
            "numberOfPages": min(200, -(-total // 20)),
            "listings": [{"id": i, "url": f"/oferte/slug-{i}", "location": {"countryCode": iso},
                          "price": {"priceRaw": 1000}} for i in ids],
        }

    async def details(self, slug):
        lid = slug.removeprefix("slug-")
        self.detail_calls.append(lid)
        self.order.append("details")
        if lid in self.gone:
            return None
        d = copy.deepcopy(self.template)
        d["id"] = lid
        d["location"]["countryCode"] = {"D": "DE", "I": "IT", "L": "LU", "N": "NL"}[lid[0]]
        return d


async def test_small_country_is_paged_and_every_listing_saved(tmp_path, used_details):
    st = Store(tmp_path / "db.sqlite")
    client = FakeClient({"L|-|-|-": 45}, used_details)
    stats = await Runner(st, client, countries=["L"], log=lambda *_: None).run()
    assert st.count() == 45
    assert stats.saved == 45
    assert sorted(p for _, p in client.search_calls) == [1, 2, 3]
    assert st.load_work() == []
    rec = next(st.iter_records())
    assert rec["source"] == "AutoScout24" and rec["make"] == "Mazda"
    assert "seller" not in st.get_raw(rec["listing_id"])


async def test_big_country_is_split_by_make(tmp_path, used_details):
    st = Store(tmp_path / "db.sqlite")
    totals = {"D|-|-|-": 5000, "D|9|-|-": 30, "D|13|-|-": 10}
    client = FakeClient(totals, used_details)
    await Runner(st, client, countries=["D"], log=lambda *_: None).run()
    searched = {k for k, _ in client.search_calls}
    assert searched == {"D|-|-|-", "D|9|-|-", "D|13|-|-"}
    assert ("D|-|-|-", 2) not in client.search_calls  # parent is not paged further
    assert st.count() == 20 + 30 + 10  # parent's first page is kept too


async def test_already_saved_listings_are_not_downloaded_again(tmp_path, used_details):
    st = Store(tmp_path / "db.sqlite")
    st.save_listings([("L|-|-|-#0", {"country": "Luxemburg"}, None)])
    client = FakeClient({"L|-|-|-": 3}, used_details)
    await Runner(st, client, countries=["L"], log=lambda *_: None).run()
    assert sorted(client.detail_calls) == ["L|-|-|-#1", "L|-|-|-#2"]


async def test_gone_listing_is_skipped_and_page_still_finishes(tmp_path, used_details):
    st = Store(tmp_path / "db.sqlite")
    client = FakeClient({"L|-|-|-": 3}, used_details, gone={"L|-|-|-#1"})
    stats = await Runner(st, client, countries=["L"], log=lambda *_: None).run()
    assert st.count() == 2
    assert stats.gone == 1
    assert st.load_work() == []


async def test_max_listings_stops_and_next_run_resumes(tmp_path, used_details):
    st = Store(tmp_path / "db.sqlite")
    totals = {"L|-|-|-": 100}
    first = FakeClient(totals, used_details)
    await Runner(st, first, countries=["L"], page_workers=1, max_listings=30, log=lambda *_: None).run()
    assert 30 <= st.count() < 100
    second = FakeClient(totals, used_details)
    await Runner(st, second, countries=["L"], log=lambda *_: None).run()
    assert st.count() == 100
    assert not set(first.detail_calls) & set(second.detail_calls)


async def test_max_listings_is_not_overshot_by_parallel_pages(tmp_path, used_details):
    st = Store(tmp_path / "db.sqlite")
    client = FakeClient({"L|-|-|-": 200}, used_details)
    await Runner(st, client, countries=["L"], page_workers=6, max_listings=30, log=lambda *_: None).run()
    assert st.count() == 30


async def test_countries_take_turns(tmp_path, used_details):
    st = Store(tmp_path / "db.sqlite")
    totals = {"D|-|-|-": 200, "I|-|-|-": 200}
    client = FakeClient(totals, used_details)
    await Runner(st, client, countries=["D", "I"], page_workers=1, log=lambda *_: None).run()
    order = [k[0] for k, _ in client.search_calls]
    assert order[:6] == ["D", "I", "D", "I", "D", "I"]


async def test_catalog_mode_saves_every_listing_from_search_pages_only(tmp_path, used_details):
    st = Store(tmp_path / "db.sqlite")
    client = FakeClient({"L|-|-|-": 45}, used_details)
    stats = await Runner(st, client, countries=["L"], mode="catalog", log=lambda *_: None).run()
    assert client.detail_calls == []
    assert st.count_by_level() == {"basic": 45}
    assert stats.saved == 45
    assert st.load_work() == []
    rec = next(st.iter_records())
    assert rec["country"] == "Luxemburg" and rec["price_euro"] == 1000 and rec["color"] is None


async def test_details_mode_upgrades_basic_listings_alternating_countries(tmp_path, used_details):
    st = Store(tmp_path / "db.sqlite")
    client = FakeClient({"D|-|-|-": 30, "I|-|-|-": 30}, used_details, gone={"I|-|-|-#3"})
    await Runner(st, client, countries=["D", "I"], mode="catalog", log=lambda *_: None).run()
    seen = {r["listing_id"]: r["first_seen_at"] for r in st.iter_records()}
    stats = await Runner(st, client, countries=["D", "I"], mode="details", detail_workers=1,
                         log=lambda *_: None).run()
    assert st.count_by_level() == {"full": 59, "gone": 1}
    assert stats.saved == 59 and stats.gone == 1
    assert [c[0] for c in client.detail_calls[:6]] == ["D", "I", "D", "I", "D", "I"]
    full = {r["listing_id"]: r for r in st.iter_records()}
    assert full["D|-|-|-#0"]["make"] == "Mazda"
    assert full["D|-|-|-#0"]["first_seen_at"] == seen["D|-|-|-#0"]  # keeps the first-seen time


async def test_auto_mode_builds_catalog_then_fills_details(tmp_path, used_details):
    st = Store(tmp_path / "db.sqlite")
    client = FakeClient({"L|-|-|-": 25}, used_details)
    await Runner(st, client, countries=["L"], mode="auto", log=lambda *_: None).run()
    assert st.count_by_level() == {"full": 25}
    last_search = max(i for i, x in enumerate(client.order) if x == "search")
    assert client.order[last_search + 1:] == ["details"] * 25  # all search pages first, then listing pages


async def test_details_mode_respects_max_listings(tmp_path, used_details):
    st = Store(tmp_path / "db.sqlite")
    client = FakeClient({"L|-|-|-": 40}, used_details)
    await Runner(st, client, countries=["L"], mode="catalog", log=lambda *_: None).run()
    await Runner(st, client, countries=["L"], mode="details", max_listings=7, log=lambda *_: None).run()
    assert st.count_by_level() == {"basic": 33, "full": 7}


class FakeAttrClient:
    """Listings with known body/color/doors; search honours the cy/body/bcol/doors filters."""

    def __init__(self, cars):
        self.cars = cars  # list of (id, cy, body, color, doors)
        self.search_calls = []

    async def bootstrap(self):
        return MAKES

    async def search(self, params):
        self.search_calls.append(params)
        hit = [c for c in self.cars if c[1] == params["cy"]
               and params.get("body", c[2]) == c[2] and params.get("bcol", c[3]) == c[3]
               and params.get("doorfrom", c[4]) == c[4]]
        page = params["page"]
        chunk = hit[(page - 1) * 20: page * 20]
        iso = {"D": "DE", "I": "IT", "L": "LU"}[params["cy"]]
        return {"numberOfResults": len(hit), "numberOfPages": -(-len(hit) // 20),
                "listings": [{"id": c[0], "url": f"/oferte/slug-{c[0]}", "location": {"countryCode": iso},
                              "price": {"priceRaw": 500}} for c in chunk]}

    async def details(self, slug):
        raise AssertionError("attrs mode must not open listing pages")


async def test_attrs_mode_fills_body_color_doors_from_filters(tmp_path):
    cars = [("a1", "L", 6, 2, 5), ("a2", "L", 6, 2, 5), ("a3", "L", 1, 14, 3), ("a4", "D", 5, 11, 5)]
    st = Store(tmp_path / "db.sqlite")
    client = FakeAttrClient(cars)
    stats = await Runner(st, client, countries=["L", "D"], mode="attrs", log=lambda *_: None).run()
    recs = {r["listing_id"]: r for r in st.iter_records()}
    assert (recs["a1"]["body_type"], recs["a1"]["color"], recs["a1"]["doors"]) == ("Berlina", "Albastru", 5)
    assert (recs["a3"]["body_type"], recs["a3"]["color"], recs["a3"]["doors"]) == ("Autoturism mic", "Alb", 3)
    assert recs["a4"]["country"] == "Germania" and recs["a4"]["color"] == "Negru"
    assert st.count_by_level() == {"attrs": 4}
    assert stats.saved == 4
    assert st.load_work() == []
    # empty branches are not explored further: no colour probes under bodies with 0 results
    probed_bodies = {p.get("body") for p in client.search_calls if "bcol" in p}
    assert probed_bodies <= {6, 1, 5}


async def test_attrs_mode_upgrades_listings_found_earlier(tmp_path, used_details):
    st = Store(tmp_path / "db.sqlite")
    st.save_basic([("a1", "slug-a1", {"listing_id": "a1", "country": "Luxemburg", "first_seen_at": "T0"})])
    await Runner(st, FakeAttrClient([("a1", "L", 4, 6, 5)]), countries=["L"], mode="attrs",
                 log=lambda *_: None).run()
    rec = next(st.iter_records())
    assert rec["body_type"] == "Off-road/4x4/Pick-up" and rec["first_seen_at"] == "T0"
