"""HTTP access to AutoScout24's Next.js JSON routes (/_next/data/<buildId>/...)."""

from __future__ import annotations

import asyncio
import json
import re
from collections import Counter

import httpx

from .limiter import AdaptiveLimiter

BASE_URL = "https://www.autoscout24.ro"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"
)
_NEXT_DATA = re.compile(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', re.S)
_THROTTLE = {403, 429, 503}


class BlockedError(RuntimeError):
    """The site kept refusing requests even after long pauses."""


def _retry_after(r: httpx.Response) -> float | None:
    try:
        return float(r.headers["Retry-After"])
    except (KeyError, ValueError):
        return None


class AS24Client:
    def __init__(
        self,
        limiter: AdaptiveLimiter,
        http: httpx.AsyncClient | None = None,
        base_url: str = BASE_URL,
        max_retries: int = 5,
        max_throttle_retries: int = 7,  # ~35 min of escalating pauses
        retry_delay: float = 2.0,
    ):
        self.limiter = limiter
        self.base_url = base_url
        self.http = http or httpx.AsyncClient(
            http2=True,
            timeout=30,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "ro-RO,ro;q=0.9"},
        )
        self.max_retries = max_retries
        self.max_throttle_retries = max_throttle_retries
        self.retry_delay = retry_delay
        self.build_id: str | None = None
        self.stats: Counter = Counter()
        self._refresh_lock = asyncio.Lock()

    async def aclose(self) -> None:
        await self.http.aclose()

    async def bootstrap(self) -> list[int]:
        """Read the current buildId and the list of make ids from a normal search page."""
        data = await self._next_data_from_html()
        return [m["value"] for m in data["props"]["pageProps"]["taxonomy"]["makesSorted"]]

    async def search(self, params: dict) -> dict | None:
        return await self._data("lst", params)

    async def details(self, slug: str) -> dict | None:
        """Listing JSON, or None when the listing no longer exists."""
        page = await self._data(f"details/{slug}", {"slug": slug})
        return (page or {}).get("listingDetails") or None

    async def _next_data_from_html(self) -> dict:
        r = await self._get(f"{self.base_url}/lst", {"atype": "C"}, follow_redirects=True)
        r.raise_for_status()
        data = json.loads(_NEXT_DATA.search(r.text).group(1))
        self.build_id = data["buildId"]
        return data

    async def _refresh_build_id(self, stale: str) -> None:
        async with self._refresh_lock:
            if self.build_id == stale:  # nobody refreshed it meanwhile
                await self._next_data_from_html()
                self.stats["build_id_refreshed"] += 1

    async def _data(self, path: str, params: dict) -> dict | None:
        refreshed = False
        while True:
            bid = self.build_id
            r = await self._get(
                f"{self.base_url}/_next/data/{bid}/{path}.json", params, headers={"x-nextjs-data": "1"}
            )
            if r.status_code == 200:
                body = r.json()
                page = body.get("pageProps") or {}
                if body.get("notFound") or "__N_REDIRECT" in page:
                    self.stats["gone"] += 1
                    return None
                return page
            if r.status_code == 404 and not refreshed:  # maybe AutoScout24 deployed a new version
                refreshed = True
                await self._refresh_build_id(bid)
                if self.build_id != bid:
                    continue
            if r.status_code in (301, 302, 307, 308, 404, 410):
                self.stats["gone"] += 1
                return None
            r.raise_for_status()
            return None

    async def _get(self, url: str, params: dict, headers: dict | None = None,
                   follow_redirects: bool = False) -> httpx.Response:
        """GET through the limiter; waits out throttling and retries network/server errors."""
        attempts = throttles = 0
        while True:
            try:
                async with self.limiter:
                    self.stats["requests"] += 1
                    r = await self.http.get(url, params=params, headers=headers,
                                            follow_redirects=follow_redirects)
            except httpx.TransportError:
                attempts += 1
                self.stats["errors"] += 1
                if attempts > self.max_retries:
                    raise
                await asyncio.sleep(self.retry_delay * 2 ** (attempts - 1))
                continue

            if r.status_code in _THROTTLE:
                throttles += 1
                self.stats["throttled"] += 1
                if throttles > self.max_throttle_retries:
                    raise BlockedError(f"HTTP {r.status_code} after {throttles - 1} pauses")
                self.limiter.throttled(_retry_after(r))
                continue
            if r.status_code >= 500:
                attempts += 1
                self.stats["errors"] += 1
                if attempts > self.max_retries:
                    r.raise_for_status()
                await asyncio.sleep(self.retry_delay * 2 ** (attempts - 1))
                continue
            self.limiter.success()
            return r
