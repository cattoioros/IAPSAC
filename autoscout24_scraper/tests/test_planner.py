import random

from carscraper.planner import (
    MAX_PRICE,
    Scheduler,
    Slice,
    split,
)

MAKES = [9, 13, 74]


def _assert_contiguous(ranges, lo, hi):
    assert ranges[0][0] == lo
    assert ranges[-1][1] == hi
    for (a_lo, a_hi), (b_lo, b_hi) in zip(ranges, ranges[1:]):
        assert a_lo <= a_hi
        assert b_lo == a_hi + 1  # inclusive bounds: no gap, no overlap


def test_slice_key_round_trip():
    for s in (Slice("D"), Slice("NL", 74), Slice("I", 9, (0, 999)), Slice("E", 13, (5, 5), (1900, 1999))):
        assert Slice.from_key(s.key) == s


def test_params_for_fully_filtered_slice():
    p = Slice("D", 74, (3000, 3499), (2017, 2017)).params(page=3)
    assert p["cy"] == "D"
    assert p["mmmv"] == "74|||"
    assert (p["pricefrom"], p["priceto"]) == (3000, 3499)
    assert (p["fregfrom"], p["fregto"]) == (2017, 2017)
    assert p["page"] == 3
    assert p["atype"] == "C"


def test_params_for_country_slice_have_no_filters():
    p = Slice("L").params(page=1)
    assert not {"mmmv", "pricefrom", "priceto", "fregfrom", "fregto"} & set(p)


def test_country_splits_into_makes():
    assert split(Slice("D"), MAKES) == [Slice("D", m) for m in MAKES]


def test_make_splits_into_contiguous_price_buckets():
    children = split(Slice("D", 74), MAKES)
    assert all(c.country == "D" and c.make_id == 74 for c in children)
    _assert_contiguous([c.price for c in children], 0, MAX_PRICE)
    assert len(children) > 10


def test_price_range_bisects():
    children = split(Slice("D", 74, (3000, 3499)), MAKES)
    _assert_contiguous([c.price for c in children], 3000, 3499)
    assert len(children) == 2


def test_single_price_splits_by_year():
    children = split(Slice("D", 74, (9990, 9990)), MAKES)
    assert all(c.price == (9990, 9990) for c in children)
    assert all(c.year is not None for c in children)
    assert len(children) == 2


def test_year_range_bisects_and_single_year_cannot_split():
    children = split(Slice("D", 74, (9990, 9990), (2010, 2013)), MAKES)
    _assert_contiguous([c.year for c in children], 2010, 2013)
    assert split(Slice("D", 74, (9990, 9990), (2017, 2017)), MAKES) == []


def test_scheduler_round_robins_countries():
    s = Scheduler(["D", "I", "NL"], rng=random.Random(1))
    for c in ("D", "I", "NL"):
        s.add(Slice(c), range(1, 4))
    order = [s.next()[0].country for _ in range(6)]
    assert order == ["D", "I", "NL", "D", "I", "NL"]


def test_scheduler_skips_countries_without_work():
    s = Scheduler(["D", "I", "NL"], rng=random.Random(1))
    s.add(Slice("NL"), [1, 2])
    assert [s.next()[0].country for _ in range(2)] == ["NL", "NL"]
    assert s.next() is None


def test_scheduler_returns_every_page_once_in_mixed_order():
    s = Scheduler(["D"], rng=random.Random(3))
    s.add(Slice("D", 9), range(1, 6))
    s.add(Slice("D", 13), range(1, 6))
    assert len(s) == 10
    got = [s.next() for _ in range(10)]
    assert sorted((sl.make_id, p) for sl, p in got) == sorted(
        [(9, p) for p in range(1, 6)] + [(13, p) for p in range(1, 6)])
    makes_in_first_half = {sl.make_id for sl, _ in got[:5]}
    assert makes_in_first_half == {9, 13}  # makes are interleaved, not one after another
    assert s.next() is None and len(s) == 0


ATTRS0 = (None, None, None)


def test_attr_slice_key_round_trip_and_differs_from_plain():
    for s in (Slice("D", attrs=ATTRS0), Slice("D", attrs=(6, None, None)),
              Slice("I", 9, (0, 999), None, (6, 2, 5)), Slice("E", 13, (5, 5), (1900, 1999), (1, 14, 3))):
        assert Slice.from_key(s.key) == s
    assert Slice("D", attrs=ATTRS0).key != Slice("D").key


def test_attr_params_use_body_color_and_doors_filters():
    p = Slice("L", attrs=(6, 2, 5)).params(page=1)
    assert (p["body"], p["bcol"], p["doorfrom"], p["doorto"]) == (6, 2, 5, 5)
    assert not {"body", "bcol", "doorfrom"} & set(Slice("L", attrs=(6, None, None)).params(1)) - {"body"}


def test_attr_slices_split_body_then_color_then_doors_then_make():
    from carscraper.planner import BODY_IDS, COLOR_IDS, DOOR_VALUES
    lvl1 = split(Slice("D", attrs=ATTRS0), MAKES)
    assert [c.attrs for c in lvl1] == [(b, None, None) for b in BODY_IDS]
    lvl2 = split(lvl1[0], MAKES)
    assert [c.attrs for c in lvl2] == [(BODY_IDS[0], c, None) for c in COLOR_IDS]
    lvl3 = split(lvl2[0], MAKES)
    assert [c.attrs[2] for c in lvl3] == DOOR_VALUES
    lvl4 = split(lvl3[0], MAKES)  # big attr slices split by price first (~30 searches, not ~300 makes)
    assert all(c.make_id is None and c.attrs == lvl3[0].attrs for c in lvl4)
    _assert_contiguous([c.price for c in lvl4], 0, MAX_PRICE)
    lvl5 = split(lvl4[0], MAKES)
    _assert_contiguous([c.price for c in lvl5], *lvl4[0].price)
    single = Slice("D", None, (9990, 9990), None, lvl3[0].attrs)
    assert [c.make_id for c in split(single, MAKES)] == MAKES  # one exact price still too big: by make
    by_make = Slice("D", 9, (9990, 9990), None, lvl3[0].attrs)
    assert all(c.year and c.make_id == 9 for c in split(by_make, MAKES))


def test_needs_split_rule():
    from carscraper.planner import needs_split
    assert needs_split(Slice("D", attrs=(6, None, None)), total=5)  # attrs must be fully fixed
    assert not needs_split(Slice("D", attrs=(6, None, None)), total=0)
    assert not needs_split(Slice("D", attrs=(6, 2, 5)), total=4000)
    assert needs_split(Slice("D", attrs=(6, 2, 5)), total=4001)
    assert not needs_split(Slice("D"), total=10) and needs_split(Slice("D"), total=5000)
