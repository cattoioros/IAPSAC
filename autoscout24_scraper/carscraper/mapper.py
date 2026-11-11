"""Convert AutoScout24 listing details into the Autovit cars.json record format."""

from __future__ import annotations

import copy
import re

BASE_URL = "https://www.autoscout24.ro"
SOURCE = "AutoScout24"

AUTOVIT_FIELDS = [
    "listing_id", "make", "model", "version", "year", "generation",
    "first_registration_date", "mileage_km", "fuel_type", "transmission",
    "body_type", "engine_displacement_cm3", "power_hp", "price_euro", "color",
    "doors", "country", "first_seen_at", "source", "features",
]

COUNTRY_NAMES = {
    "DE": "Germania", "IT": "Italia", "NL": "Olanda", "BE": "Belgia",
    "FR": "Franta", "AT": "Austria", "ES": "Spania", "LU": "Luxemburg",
}

RO_MONTHS = [
    "ianuarie", "februarie", "martie", "aprilie", "mai", "iunie", "iulie",
    "august", "septembrie", "octombrie", "noiembrie", "decembrie",
]

_RO_DIACRITICS = str.maketrans("ăâîșşțţĂÂÎȘŞȚŢ", "aaissttAAISSTT")

# Keys holding seller personal data; never stored.
_PRIVATE_KEYS = ("seller", "whatsappNumber", "description")
_PRIVATE_LOCATION_KEYS = ("street", "latitude", "longitude")


def strip_ro(value):
    """Remove Romanian diacritics so values match Autovit's spelling (Manuală -> Manuala)."""
    return value.translate(_RO_DIACRITICS) if isinstance(value, str) else value


def format_first_registration(raw: str | None) -> str | None:
    """'2017-08-01' -> 'august 2017' (AutoScout24 only gives month and year)."""
    try:
        year, month = int(raw[:4]), int(raw[5:7])
        return f"{RO_MONTHS[month - 1]} {year}"
    except (TypeError, ValueError, IndexError):
        return None


def _version(v: dict) -> str | None:
    if v.get("modelVersionInput"):
        return v["modelVersionInput"]
    parts = [p for p in (v.get("variant"), v.get("motorTypeName")) if p]
    return " ".join(parts) or None


def _year(v: dict) -> int | None:
    raw = v.get("firstRegistrationDateRaw")
    if raw and raw[:4].isdigit():
        return int(raw[:4])
    return v.get("productionYear") or None


def _features(v: dict) -> list[str] | None:
    feats = [strip_ro(item["id"]) for items in (v.get("equipment") or {}).values() for item in items]
    return feats or None


def to_record(details: dict, slug: str, first_seen_at: str) -> dict:
    v = details.get("vehicle") or {}
    prices = (details.get("prices") or {}).get("public") or {}
    country_code = (details.get("location") or {}).get("countryCode")
    return {
        "listing_id": details["id"],
        "make": v.get("make"),
        "model": v.get("model"),
        "version": _version(v),
        "year": _year(v),
        "generation": None,  # AutoScout24 exposes only a numeric generation id
        "first_registration_date": format_first_registration(v.get("firstRegistrationDateRaw")),
        "mileage_km": v.get("mileageInKmRaw"),
        "fuel_type": strip_ro((v.get("fuelCategory") or {}).get("formatted")),
        "transmission": strip_ro(v.get("transmissionType")),
        "body_type": strip_ro(v.get("bodyType")),
        "engine_displacement_cm3": v.get("rawDisplacementInCCM"),
        "power_hp": v.get("rawPowerInHp"),
        "price_euro": prices.get("priceRaw"),
        "color": strip_ro(v.get("bodyColor")),
        "doors": v.get("numberOfDoors"),
        "country": COUNTRY_NAMES.get(country_code, country_code),
        "first_seen_at": first_seen_at,
        "source": SOURCE,
        "features": _features(v),
        "url": f"{BASE_URL}/oferte/{slug}",
    }


_POWER_HP = re.compile(r"\((\d[\d.]*)\s*CP\)")


def _int_digits(value) -> int | None:
    digits = re.sub(r"\D", "", value) if isinstance(value, str) else ""
    return int(digits) if digits else None


def basic_record(listing: dict, first_seen_at: str) -> dict:
    """Record from a search-result entry; fields only shown on the listing page stay None."""
    v = listing.get("vehicle") or {}
    tracking = listing.get("tracking") or {}
    reg = tracking.get("firstRegistration") or ""  # "04-2016"
    reg_raw = f"{reg[3:7]}-{reg[:2]}-01" if re.fullmatch(r"\d\d-\d{4}", reg) else None
    power = next((d.get("data") for d in listing.get("vehicleDetails") or []
                  if d.get("iconName") == "speedometer"), None)
    m = _POWER_HP.search(power or "")
    country_code = (listing.get("location") or {}).get("countryCode")
    slug = (listing.get("url") or "").rsplit("/", 1)[-1]
    return {
        "listing_id": listing["id"],
        "make": v.get("make"),
        "model": v.get("model"),
        "version": _version(v),
        "year": int(reg_raw[:4]) if reg_raw else None,
        "generation": None,
        "first_registration_date": format_first_registration(reg_raw),
        "mileage_km": _int_digits(tracking.get("mileage")),
        "fuel_type": strip_ro(v.get("fuel")),
        "transmission": strip_ro(v.get("transmission")),
        "body_type": None,
        "engine_displacement_cm3": _int_digits(v.get("engineDisplacementInCCM")),
        "power_hp": _int_digits(m.group(1)) if m else None,
        "price_euro": (listing.get("price") or {}).get("priceRaw"),
        "color": None,
        "doors": None,
        "country": COUNTRY_NAMES.get(country_code, country_code),
        "first_seen_at": first_seen_at,
        "source": SOURCE,
        "features": None,
        "url": f"{BASE_URL}/oferte/{slug}",
    }


def sanitize_details(details: dict) -> dict:
    """Copy of the raw listing without seller personal data, for archiving."""
    clean = copy.deepcopy(details)
    for key in _PRIVATE_KEYS:
        clean.pop(key, None)
    for key in _PRIVATE_LOCATION_KEYS:
        (clean.get("location") or {}).pop(key, None)
    return clean
