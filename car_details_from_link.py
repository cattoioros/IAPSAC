import playwright.sync_api
import json
import re
import datetime


def parse_number(value):
    """Convert values such as ``49 142 km`` or ``25 990`` to an integer."""
    if not value:
        return None

    number_match = re.match(r"\s*(\d[\d\s.]*)", value)
    digits = re.sub(r"\D", "", number_match.group(1)) if number_match else ""
    return int(digits) if digits else None


def build_listing_dictionary(general, price, details, equipment, url):
    """Normalize the scraped sections into the dataset's listing schema."""
    first_registration = details.get("Data primei inmatriculari")
    features = [
        item
        for category_items in equipment.values()
        for item in category_items
    ]

    listing_match = re.search(r"-([A-Za-z0-9]+)\.html", url)

    return {
        "listing_id": listing_match.group(1) if listing_match else None,
        "make": details.get("Marca"),
        "model": details.get("Model"),
        "version": details.get("Versiune"),
        "year": parse_number(details.get("Anul producției")),
        "generation": details.get("Generatie"),
        "first_registration_date": first_registration,
        "mileage_km": parse_number(general[0] if len(general) > 0 else None),
        "fuel_type": general[1] if len(general) > 1 else None,
        "transmission": general[2] if len(general) > 2 else None,
        "body_type": general[3] if len(general) > 3 else None,
        "engine_displacement_cm3": parse_number(
            general[4][:-1] if len(general) > 4 else None
        ),
        "power_hp": parse_number(general[5] if len(general) > 5 else None),
        "price_euro": parse_number(price[0] if price else None),
        "color": details.get("Culoare"),
        "doors": parse_number(details.get("Numar de portiere")),
        "country": "Romania",
        "first_seen_at": str(datetime.datetime.now()),
        "source": "Autovit.ro",
        "features": features,
    }



def extract_car_details(page, url):
    page.goto(url)

    general = page.locator("p.FxCMd.t9B-P.CgHmQ").all_inner_texts()

    general = general[:len(general)-4:2] #ultimele 4 detalii sunt irelevante, punem din 2 in 2 sa eliminam redundanta

    price = page.locator("aside h3.offer-price__number").all_inner_texts() #doar pret nornmal, fara pret net in caz de deducere tva



    container = page.locator('[data-testid="basic_information"]')
    rows = container.locator('[data-testid]')
    details = {}
    for i in range(rows.count()):
        row = rows.nth(i)

        values = row.locator("p").all_inner_texts()

        if len(values) == 2:
            key = values[0]
            value = values[1]

            details[key] = value




    section = page.locator('[data-testid="content-equipments-section"]')
    buttons = section.locator("button")
    equipment = {}

    for i in range(buttons.count()):
        button = buttons.nth(i)
        category = button.inner_text().strip()

        if button.get_attribute("aria-expanded") == "false":
            button.click()

        content = button.locator("xpath=following-sibling::div[1]")
        items = content.locator("p").all_inner_texts()

        equipment[category] = items



    listing = build_listing_dictionary(
        general,
        price,
        details,
        equipment,
        page.url,
    )
    return listing
