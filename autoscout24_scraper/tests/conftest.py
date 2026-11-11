import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture
def used_details() -> dict:
    return load_fixture("details_used.json")["pageProps"]["listingDetails"]


@pytest.fixture
def new_details() -> dict:
    return load_fixture("details_new.json")["pageProps"]["listingDetails"]


@pytest.fixture
def search_page() -> dict:
    return load_fixture("search_page.json")["pageProps"]
