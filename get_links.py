from playwright.sync_api import sync_playwright
import json
import time

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)

    context = browser.new_context(
        storage_state="state.json"
    )
    page = context.new_page()

    all_links = []


    for i in range(1,11):        

        page.goto(f"https://www.autovit.ro/autoturisme/?page={i}")

        links = page.locator("article h2 a").evaluate_all(
        "(elements) => elements.map(a => a.href)")

        all_links.extend(links)
        time.sleep(10)

    with open("links.json", "w", encoding="utf-8") as f:
        json.dump(all_links, f, indent=4, ensure_ascii=False)


    browser.close()