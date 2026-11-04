from playwright.sync_api import sync_playwright
import car_details_from_link as car
import json
import time
import random

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)

    context = browser.new_context(storage_state="state.json")
    page = context.new_page()


    cars = []


    with open("links.json", encoding="utf-8") as f:
        links = json.load(f)

    # for link in links:
    for i in range(2):

        time.sleep(random.randint(1,3))
        listing = car.extract_car_details(page, links[i])
        cars.append(listing)


    with open("cars.json", "w", encoding="utf-8") as f:
        json.dump(cars, f, indent=4, ensure_ascii=False)

        

    print(f"Au fost salvate {len(cars)} anunturi in cars.json.")

    browser.close()