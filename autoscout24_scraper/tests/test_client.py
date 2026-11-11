import json

import httpx
import pytest

from carscraper.client import AS24Client, BlockedError
from carscraper.limiter import AdaptiveLimiter

HTML = (
    '<html><script id="__NEXT_DATA__" type="application/json">'
    + json.dumps({"buildId": "{bid}", "props": {"pageProps": {"taxonomy": {
        "makesSorted": [{"label": "Audi", "value": 9}, {"label": "VW", "value": 74}]}}}})
    + "</script></html>"
)


def make_client(handler, **lim):
    limiter = AdaptiveLimiter(base_pause=0.01, **lim)
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://as24.test")
    return AS24Client(limiter, http=http, base_url="https://as24.test", retry_delay=0), limiter


def html_for(bid):
    return httpx.Response(200, text=HTML.replace("{bid}", bid))


async def test_bootstrap_reads_build_id_and_makes():
    def handler(req):
        assert req.url.path == "/lst"
        return html_for("b1")
    client, _ = make_client(handler)
    assert await client.bootstrap() == [9, 74]
    assert client.build_id == "b1"


async def test_bootstrap_waits_and_retries_when_throttled():
    calls = {"n": 0}

    def handler(req):
        calls["n"] += 1
        return httpx.Response(429) if calls["n"] == 1 else html_for("b1")
    client, limiter = make_client(handler, start=10)
    assert await client.bootstrap() == [9, 74]
    assert limiter.limit == 5


async def test_search_uses_build_id_and_returns_page_props():
    seen = []

    def handler(req):
        if req.url.path == "/lst":
            return html_for("b1")
        seen.append(req)
        return httpx.Response(200, json={"pageProps": {"numberOfResults": 5, "listings": []}})
    client, _ = make_client(handler)
    await client.bootstrap()
    page = await client.search({"cy": "D", "page": 2})
    assert page["numberOfResults"] == 5
    assert seen[0].url.path == "/_next/data/b1/lst.json"
    assert seen[0].url.params["cy"] == "D"
    assert seen[0].headers["x-nextjs-data"] == "1"


async def test_stale_build_id_is_refreshed_and_request_retried():
    bids = iter(["old", "new"])

    def handler(req):
        if req.url.path == "/lst":
            return html_for(next(bids))
        if "/old/" in req.url.path:
            return httpx.Response(404)
        return httpx.Response(200, json={"pageProps": {"listingDetails": {"id": "x"}}})
    client, _ = make_client(handler)
    await client.bootstrap()
    assert await client.details("slug-x") == {"id": "x"}
    assert client.build_id == "new"


async def test_details_404_with_current_build_id_means_listing_gone():
    def handler(req):
        if req.url.path == "/lst":
            return html_for("b1")
        return httpx.Response(404)
    client, _ = make_client(handler)
    await client.bootstrap()
    assert await client.details("gone") is None


async def test_details_redirect_means_listing_gone():
    def handler(req):
        if req.url.path == "/lst":
            return html_for("b1")
        return httpx.Response(200, json={"pageProps": {"__N_REDIRECT": "/lst", "__N_REDIRECT_STATUS": 301}})
    client, _ = make_client(handler)
    await client.bootstrap()
    assert await client.details("moved") is None


async def test_throttling_slows_down_then_retries():
    calls = {"n": 0}

    def handler(req):
        if req.url.path == "/lst":
            return html_for("b1")
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json={"pageProps": {"listingDetails": {"id": "y"}}})
    client, limiter = make_client(handler, start=10)
    await client.bootstrap()
    assert await client.details("slug-y") == {"id": "y"}
    assert limiter.limit == 5
    assert client.stats["throttled"] == 1


async def test_persistent_blocking_raises_blocked_error():
    def handler(req):
        if req.url.path == "/lst":
            return html_for("b1")
        return httpx.Response(403)
    client, _ = make_client(handler)
    client.max_throttle_retries = 3
    await client.bootstrap()
    with pytest.raises(BlockedError):
        await client.details("slug")


async def test_server_errors_are_retried_then_raised():
    calls = {"n": 0}

    def handler(req):
        if req.url.path == "/lst":
            return html_for("b1")
        calls["n"] += 1
        return httpx.Response(500)
    client, _ = make_client(handler)
    await client.bootstrap()
    with pytest.raises(httpx.HTTPStatusError):
        await client.search({"cy": "D"})
    assert calls["n"] == client.max_retries + 1
