from carscraper.mapper import (
    AUTOVIT_FIELDS,
    basic_record,
    format_first_registration,
    sanitize_details,
    strip_ro,
    to_record,
)

SEEN = "2026-11-09 15:00:00.000000"
SLUG = "mazda-3-exclusive-line-motorina-negru-cat_ma46mo18388-7a3345b0-442a-405e-bd99-8722495d1655"


def test_record_has_autovit_fields_in_order_plus_url(used_details):
    rec = to_record(used_details, SLUG, SEEN)
    assert list(rec) == AUTOVIT_FIELDS + ["url"]


def test_used_car_maps_to_autovit_values(used_details):
    rec = to_record(used_details, SLUG, SEEN)
    assert rec["listing_id"] == "7a3345b0-442a-405e-bd99-8722495d1655"
    assert rec["make"] == "Mazda"
    assert rec["model"] == "3"
    assert rec["version"] == "Exclusive-Line"
    assert rec["year"] == 2017
    assert rec["generation"] is None
    assert rec["first_registration_date"] == "august 2017"
    assert rec["mileage_km"] == 222158
    assert rec["fuel_type"] == "Motorina"
    assert rec["transmission"] == "Manuala"
    assert rec["body_type"] == "Autoturism mic"
    assert rec["engine_displacement_cm3"] == 2191
    assert rec["power_hp"] == 150
    assert rec["price_euro"] == 3490
    assert rec["color"] == "Negru"
    assert rec["doors"] == 5
    assert rec["country"] == "Germania"
    assert rec["first_seen_at"] == SEEN
    assert rec["source"] == "AutoScout24"
    assert rec["url"] == f"https://www.autoscout24.ro/oferte/{SLUG}"


def test_features_are_flattened_without_romanian_diacritics(used_details):
    feats = to_record(used_details, SLUG, SEEN)["features"]
    assert len(feats) == 42
    assert "Aer conditionat automat" in feats
    assert all("ţ" not in f and "ă" not in f for f in feats)


def test_new_car_without_registration_has_null_year_and_date(new_details):
    rec = to_record(new_details, "x", SEEN)
    assert rec["year"] is None
    assert rec["first_registration_date"] is None
    assert rec["fuel_type"] == "Electric/Benzina"
    assert rec["transmission"] == "Automata"
    assert rec["country"] == "Italia"


def test_missing_equipment_gives_null_features(used_details):
    used_details["vehicle"]["equipment"] = {}
    assert to_record(used_details, SLUG, SEEN)["features"] is None


def test_version_falls_back_to_variant_and_motor(used_details):
    v = used_details["vehicle"]
    v["modelVersionInput"] = None
    v["variant"] = "Golf Convertible"
    v["motorTypeName"] = "1.4 TSI"
    assert to_record(used_details, SLUG, SEEN)["version"] == "Golf Convertible 1.4 TSI"


def test_format_first_registration():
    assert format_first_registration("2023-02-01") == "februarie 2023"
    assert format_first_registration("2011-12-01") == "decembrie 2011"
    assert format_first_registration(None) is None
    assert format_first_registration("garbage") is None


def test_strip_ro_removes_only_romanian_diacritics():
    assert strip_ro("Manuală Încălzire ştergător Ţară") == "Manuala Incalzire stergator Tara"
    assert strip_ro("Citroën") == "Citroën"
    assert strip_ro(None) is None


def test_sanitize_drops_seller_personal_data(used_details):
    used_details["seller"] = {"phones": ["+49 123"], "contactName": "X"}
    used_details["whatsappNumber"] = "+49 123"
    used_details["description"] = "call me"
    used_details["location"].update(street="Str 1", latitude=1.0, longitude=2.0)
    clean = sanitize_details(used_details)
    for key in ("seller", "whatsappNumber", "description"):
        assert key not in clean
    assert clean["location"] == {k: v for k, v in used_details["location"].items()
                                 if k not in ("street", "latitude", "longitude")}
    assert "seller" in used_details  # input is not mutated


def test_basic_record_from_search_listing(search_page):
    lancia = next(l for l in search_page["listings"] if l["id"].startswith("75bf532e"))
    rec = basic_record(lancia, SEEN)
    assert list(rec) == AUTOVIT_FIELDS + ["url"]
    assert rec["listing_id"] == lancia["id"]
    assert (rec["make"], rec["model"]) == ("Lancia", "Ypsilon")
    assert rec["version"] == "Ypsilon 1.2 Silver Gpl 69cv"
    assert rec["year"] == 2016
    assert rec["first_registration_date"] == "aprilie 2016"
    assert rec["mileage_km"] == 91649
    assert rec["fuel_type"] == "GPL"
    assert rec["transmission"] == "Manuala"
    assert rec["engine_displacement_cm3"] == 1242
    assert rec["power_hp"] == 69
    assert rec["price_euro"] == 6900
    assert rec["country"] == "Italia"
    assert rec["source"] == "AutoScout24"
    assert rec["url"].endswith(lancia["url"].rsplit("/", 1)[-1])
    for missing in ("generation", "body_type", "color", "doors", "features"):
        assert rec[missing] is None


def test_basic_record_tolerates_missing_values(search_page):
    lst = search_page["listings"][0]
    lst["tracking"] = {}
    lst["vehicleDetails"] = []
    lst["vehicle"]["engineDisplacementInCCM"] = None
    rec = basic_record(lst, SEEN)
    assert rec["year"] is None and rec["mileage_km"] is None
    assert rec["power_hp"] is None and rec["engine_displacement_cm3"] is None


def test_every_fixture_search_listing_maps(search_page):
    recs = [basic_record(l, SEEN) for l in search_page["listings"]]
    assert all(r["price_euro"] and r["make"] and r["country"] for r in recs)
    assert sum(r["power_hp"] is not None for r in recs) >= 15
