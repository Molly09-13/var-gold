from __future__ import annotations

import time
from typing import Any

import requests

from .models import MarketSnapshot
from .utils import now_utc_ms, safe_float


class CollectorError(RuntimeError):
    pass


class MarketCollector:
    def __init__(self, api_url: str, quote_size: str, annual_factor: float, timeout_sec: int = 10) -> None:
        self.api_url = api_url
        self.quote_size = quote_size
        self.annual_factor = annual_factor
        self.timeout_sec = timeout_sec

    def fetch_snapshot(self) -> MarketSnapshot:
        start = time.perf_counter()
        try:
            resp = requests.get(self.api_url, timeout=self.timeout_sec)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            raise CollectorError(f"API request failed: {exc}") from exc
        latency_ms = int((time.perf_counter() - start) * 1000)

        if not isinstance(payload, dict):
            raise CollectorError("API payload is not an object")

        listings = payload.get("listings")
        if not isinstance(listings, list):
            raise CollectorError("API payload missing listings list")

        paxg = self._find_listing(listings, "PAXG")
        xaut = self._find_listing(listings, "XAUT")
        if not paxg or not xaut:
            raise CollectorError("PAXG or XAUT listing missing")

        paxg_quote = self._extract_quote(paxg)
        xaut_quote = self._extract_quote(xaut)
        if not paxg_quote or not xaut_quote:
            raise CollectorError("Missing quote data for PAXG/XAUT")

        paxg_bid, paxg_ask, paxg_quote_size = paxg_quote
        xaut_bid, xaut_ask, xaut_quote_size = xaut_quote

        spread_open = paxg_bid - xaut_ask
        spread_close = xaut_bid - paxg_ask

        paxg_funding = safe_float(paxg.get("funding_rate"))
        xaut_funding = safe_float(xaut.get("funding_rate"))

        funding_diff_raw: float | None
        funding_diff_annual: float | None
        if paxg_funding is None or xaut_funding is None:
            funding_diff_raw = None
            funding_diff_annual = None
        else:
            funding_diff_raw = paxg_funding - xaut_funding
            funding_diff_annual = funding_diff_raw * self.annual_factor

        return MarketSnapshot(
            ts_ms=now_utc_ms(),
            paxg_bid=paxg_bid,
            paxg_ask=paxg_ask,
            xaut_bid=xaut_bid,
            xaut_ask=xaut_ask,
            spread_open=spread_open,
            spread_close=spread_close,
            paxg_funding=paxg_funding,
            xaut_funding=xaut_funding,
            funding_diff_raw=funding_diff_raw,
            funding_diff_annual=funding_diff_annual,
            annual_factor=self.annual_factor,
            quote_size_paxg=paxg_quote_size,
            quote_size_xaut=xaut_quote_size,
            latency_ms=latency_ms,
        )

    @staticmethod
    def _find_listing(listings: list[Any], ticker: str) -> dict[str, Any] | None:
        for item in listings:
            if isinstance(item, dict) and item.get("ticker") == ticker:
                return item
        return None

    def _extract_quote(self, listing: dict[str, Any]) -> tuple[float, float, str] | None:
        quotes = listing.get("quotes")
        if not isinstance(quotes, dict) or not quotes:
            return None

        preferred = self._extract_quote_level(quotes.get(self.quote_size))
        if preferred:
            bid, ask = preferred
            return bid, ask, self.quote_size

        for key in sorted(quotes.keys(), key=lambda v: str(v)):
            parsed = self._extract_quote_level(quotes.get(key))
            if parsed:
                bid, ask = parsed
                return bid, ask, str(key)

        return None

    @staticmethod
    def _extract_quote_level(quote: Any) -> tuple[float, float] | None:
        if not isinstance(quote, dict):
            return None
        bid = safe_float(quote.get("bid"))
        ask = safe_float(quote.get("ask"))
        if bid is None or ask is None:
            return None
        return bid, ask
